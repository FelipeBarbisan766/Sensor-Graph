#!/usr/bin/env python3
"""
sensor_inserter.py

Insere dados na tabela `sensorsdata` a cada 4 segundos.
Possui uma probabilidade configurável para gerar leituras totalmente aleatórias.
Caso a leitura não seja aleatória, os valores serão variações pequenas das leituras anteriores.

Dependências:
    pip install mysql-connector-python

Configurar as credenciais do banco no dicionário `DB_CONFIG`.
"""

import time
import random
import logging
import signal
import sys
from decimal import Decimal, ROUND_HALF_UP

import mysql.connector
from mysql.connector import Error

# ================== CONFIGURAÇÃO ==================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "graphtest",
    "port": 3306,
}

SLEEP_SECONDS = 4  # intervalo entre inserts
RANDOM_PROBABILITY = 0.3  # probabilidade de gerar leitura totalmente aleatória (0..1)
SENSOR_IDS = ["sensor_1"]  # ids possíveis dos sensores (escolhidos aleatoriamente)
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

stop_requested = False


def decimal_round(value, ndigits=2):
    """
    Arredonda um float para Decimal com ndigits casas decimais, retornando Decimal.
    Isso ajuda a manter conformidade com NUMERIC(5,2) / NUMERIC(3,2).
    """
    q = Decimal(10) ** -ndigits
    return Decimal(value).quantize(q, rounding=ROUND_HALF_UP)


def connect_db():
    """Tenta conectar ao banco e retorna uma conexão."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            logging.info("Conectado ao banco de dados")
            return conn
    except Error as e:
        logging.error(f"Erro ao conectar ao banco: {e}")
    return None


def generate_random_reading():
    """Gera uma leitura totalmente aleatória dentro de intervalos plausíveis."""
    umidade = round(random.uniform(0, 100), 2)  # % de umidade
    temperatura = decimal_round(random.uniform(-10.0, 50.0), 2)  # °C (Decimal)
    ph = decimal_round(random.uniform(0.0, 14.0), 2)  # pH 0..14 (Decimal)
    latitude = round(random.uniform(-90.0, 90.0), 6)
    longetude = round(random.uniform(-180.0, 180.0), 6)
    return umidade, temperatura, ph, latitude, longetude


def vary_reading(prev, max_delta=1.0):
    """
    Gera uma nova leitura a partir da anterior aplicando pequenas variações gaussianas.
    prev: tuple (umidade, temperatura, ph, latitude, longetude)
    """
    if prev is None:
        return generate_random_reading()

    umidade_prev, temperatura_prev, ph_prev, lat_prev, lon_prev = prev

    # Corrige mistura de Decimal e float convertendo para float antes da operação
    temperatura_prev = float(temperatura_prev)
    ph_prev = float(ph_prev)

    umidade = min(100.0, max(0.0, umidade_prev + random.gauss(0, 1.5)))
    temperatura = temperatura_prev + random.gauss(0, 0.3)
    temperatura = float(decimal_round(temperatura, 2))
    ph = ph_prev + random.gauss(0, 0.05)
    ph = float(max(0.0, min(14.0, decimal_round(ph, 2))))
    latitude = lat_prev + random.gauss(0, 0.00001)
    longetude = lon_prev + random.gauss(0, 0.00001)

    return round(umidade, 2), temperatura, ph, round(latitude, 6), round(longetude, 6)


def insert_reading(conn, sensor_id, umidade, temperatura_c, ph, latitude, longetude):
    """
    Insere uma linha na tabela sensorsdata.
    Observação: a coluna recorded_at tem DEFAULT now(), então aqui não precisamos enviar timestamp.
    """
    sql = """
        INSERT INTO sensorsdata (sensor_id, umidade, temperatura_c, ph, latitude, longetude)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    values = (sensor_id, umidade, float(temperatura_c), float(ph), latitude, longetude)
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        cur.close()
        logging.info(
            f"INSERT sensor={sensor_id} umidade={umidade} temp={temperatura_c} ph={ph} lat={latitude} lon={longetude}"
        )
    except Error as e:
        logging.error(f"Erro no INSERT: {e}")
        # tenta rolar para trás se necessário
        try:
            conn.rollback()
        except Exception:
            pass


def handle_shutdown(signum, frame):
    global stop_requested
    logging.info("Recebido sinal de término, finalizando...")
    stop_requested = True


def main():
    global stop_requested
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    conn = connect_db()
    if conn is None:
        logging.error("Não foi possível conectar ao banco. Saindo.")
        return

    last_reading = None

    # Inicializa last_reading com um valor aleatório plausível para modo de variação
    last_reading = generate_random_reading()

    try:
        while not stop_requested:
            sensor_id = random.choice(SENSOR_IDS)

            if random.random() < RANDOM_PROBABILITY:
                # leitura totalmente aleatória
                umidade, temperatura, ph, lat, lon = generate_random_reading()
            else:
                # pequena variação a partir da última leitura
                umidade, temperatura, ph, lat, lon = vary_reading(last_reading)

            # garante formatação correta (temperatura e ph como Decimal/float com 2 casas)
            temperatura = decimal_round(temperatura, 2)
            ph = decimal_round(ph, 2)

            insert_reading(conn, sensor_id, umidade, temperatura, ph, lat, lon)

            # atualiza última leitura para o sensor (poderíamos ter last por sensor; aqui usamos único)
            last_reading = (umidade, float(temperatura), float(ph), lat, lon)

            # espera N segundos
            for _ in range(int(SLEEP_SECONDS * 10)):
                if stop_requested:
                    break
                time.sleep(0.1)

    finally:
        try:
            if conn.is_connected():
                conn.close()
                logging.info("Conexão com banco fechada.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
