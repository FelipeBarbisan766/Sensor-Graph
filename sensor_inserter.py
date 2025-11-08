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
from datetime import datetime
from typing import Dict, Tuple, Optional

import mysql.connector
from mysql.connector import Error

# ================== CONFIGURAÇÃO ==================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "soil_sensors",
    "port": 3306,
}

SENSOR_NAMES = ["Sensor-01"]  # lista de sensores
INTERVAL_SECONDS = 4  # intervalo entre inserts
RANDOM_PROBABILITY = 0.25  # probabilidade de gerar leitura totalmente aleatória
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

stop_requested = False


def connect_db():
    """Tenta conectar ao MySQL e retorna a conexão (ou None em falha)."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            logging.info("Conectado ao banco de dados")
            return conn
    except Error as e:
        logging.error(f"Erro ao conectar ao banco: {e}")
    return None


def now_str():
    """Timestamp formatado para inserção (YYYY-MM-DD HH:MM:SS)."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def generate_random_reading():
    """
    Gera leitura totalmente aleatória com intervalos plausíveis.
    Retorna tuple: (latitude, longitude, moisture, temperature, ph, ec, nitrogen, phosphorus, potassium)
    """
    latitude = round(random.uniform(-90.0, 90.0), 7)
    longitude = round(random.uniform(-180.0, 180.0), 7)
    moisture = round(random.uniform(0.0, 100.0), 2)  # %
    temperature = round(random.uniform(-10.0, 50.0), 2)  # °C
    ph = round(random.uniform(0.0, 14.0), 2)  # pH
    ec = round(random.uniform(0.0, 5.0), 3)  # dS/m
    nitrogen = round(random.uniform(0.0, 100.0), 3)  # unidade arbitrária
    phosphorus = round(random.uniform(0.0, 100.0), 3)
    potassium = round(random.uniform(0.0, 200.0), 3)
    return (
        latitude,
        longitude,
        moisture,
        temperature,
        ph,
        ec,
        nitrogen,
        phosphorus,
        potassium,
    )


def vary_reading(prev: Tuple[float, ...]) -> Tuple[float, ...]:
    """
    Gera nova leitura a partir da anterior aplicando pequenas variações gaussianas.
    prev: (latitude, longitude, moisture, temperature, ph, ec, nitrogen, phosphorus, potassium)
    """
    (
        lat_prev,
        lon_prev,
        moisture_prev,
        temp_prev,
        ph_prev,
        ec_prev,
        n_prev,
        p_prev,
        k_prev,
    ) = prev

    latitude = round(lat_prev + random.gauss(0, 0.00001), 7)
    longitude = round(lon_prev + random.gauss(0, 0.00001), 7)

    moisture = round(max(0.0, min(100.0, moisture_prev + random.gauss(0, 0.5))), 2)
    temperature = round(temp_prev + random.gauss(0, 0.15), 2)
    ph = round(max(0.0, min(14.0, ph_prev + random.gauss(0, 0.02))), 2)
    ec = round(max(0.0, ec_prev + random.gauss(0, 0.01)), 3)
    nitrogen = round(max(0.0, n_prev + random.gauss(0, 0.05)), 3)
    phosphorus = round(max(0.0, p_prev + random.gauss(0, 0.03)), 3)
    potassium = round(max(0.0, k_prev + random.gauss(0, 0.1)), 3)

    return (
        latitude,
        longitude,
        moisture,
        temperature,
        ph,
        ec,
        nitrogen,
        phosphorus,
        potassium,
    )


def insert_reading(
    conn,
    sensor_name: str,
    recorded_at: str,
    latitude: float,
    longitude: float,
    moisture: float,
    temperature: float,
    ph: float,
    ec: float,
    nitrogen: float,
    phosphorus: float,
    potassium: float,
) -> bool:
    """
    Executa o INSERT na tabela sensor_readings.
    Retorna True se inseriu com sucesso, False caso contrário.
    """
    sql = """
        INSERT INTO sensor_readings (
            sensor_name, recorded_at, latitude, longitude,
            moisture, temperature, ph, ec, nitrogen, phosphorus, potassium
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        sensor_name,
        recorded_at,
        latitude,
        longitude,
        moisture,
        temperature,
        ph,
        ec,
        nitrogen,
        phosphorus,
        potassium,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        cur.close()
        logging.info(
            f"INSERT sensor={sensor_name} at={recorded_at} lat={latitude} lon={longitude} "
            f"moisture={moisture} temp={temperature} pH={ph} EC={ec} N={nitrogen} P={phosphorus} K={potassium}"
        )
        return True
    except Error as e:
        logging.error(f"Erro no INSERT: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def handle_shutdown(signum, frame):
    global stop_requested
    logging.info("Sinal de término recebido, finalizando...")
    stop_requested = True


def main():
    global stop_requested
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    conn = connect_db()
    if conn is None:
        logging.error("Não foi possível conectar ao banco. Saindo.")
        return

    # Guarda a última leitura por sensor
    last_readings: Dict[str, Tuple[float, ...]] = {}
    for s in SENSOR_NAMES:
        last_readings[s] = generate_random_reading()

    try:
        while not stop_requested:
            for sensor in SENSOR_NAMES:
                if random.random() < RANDOM_PROBABILITY:
                    # leitura totalmente aleatória
                    lat, lon, moisture, temp, ph, ec, n, p, k = (
                        generate_random_reading()
                    )
                else:
                    # pequena variação a partir da última leitura desse sensor
                    lat, lon, moisture, temp, ph, ec, n, p, k = vary_reading(
                        last_readings[sensor]
                    )

                recorded_at = now_str()

                # tenta inserir; se falhar por conexão, tenta reconectar e reenviar uma vez
                success = insert_reading(
                    conn, sensor, recorded_at, lat, lon, moisture, temp, ph, ec, n, p, k
                )
                if not success:
                    # tenta reconectar e reenviar uma vez
                    try:
                        if conn.is_connected():
                            conn.close()
                    except Exception:
                        pass
                    conn = connect_db()
                    if conn:
                        insert_reading(
                            conn,
                            sensor,
                            recorded_at,
                            lat,
                            lon,
                            moisture,
                            temp,
                            ph,
                            ec,
                            n,
                            p,
                            k,
                        )

                last_readings[sensor] = (lat, lon, moisture, temp, ph, ec, n, p, k)

                # interrompe cedo se solicitado
                if stop_requested:
                    break

            # espera INTERVAL_SECONDS segundos (pequeno intervalo entre sensores não adicionado; o loop total faz ~INTERVAL_SECONDS)
            for _ in range(int(INTERVAL_SECONDS * 10)):
                if stop_requested:
                    break
                time.sleep(0.1)

    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
                logging.info("Conexão com banco fechada.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
