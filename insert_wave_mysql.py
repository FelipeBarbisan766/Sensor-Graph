#!/usr/bin/env python3
"""
insert_wave_mysql.py

Insere leituras na tabela `ground_sensor_readings` a cada N segundos,
gerando valores em formato de onda (seno).

Requisitos:
    pip install mysql-connector-python

Uso:
    python insert_wave_mysql.py --host localhost --port 3306 --database seu_db --user seu_usuario --password sua_senha --sensor-id ground-01

Opções úteis:
    --interval       Intervalo entre inserts em segundos (default 4)
    --period         Período da onda em segundos (default 60)
    --create-table   Cria a tabela se ela não existir
"""

import time
import math
import random
import argparse
import sys
from contextlib import closing

try:
    import mysql.connector
    from mysql.connector import errors as db_errors
except ImportError:
    print("mysql-connector-python não encontrado. Instale com: pip install mysql-connector-python")
    sys.exit(1)

INSERT_SQL = """
INSERT INTO ground_sensor_readings
  (sensor_id, moisture_pct, temperature_c, ph, battery_voltage_v)
VALUES
  (%s, %s, %s, %s, %s)
"""

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ground_sensor_readings (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  sensor_id VARCHAR(255) NOT NULL,
  recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  moisture_pct DECIMAL(5,2) NOT NULL,
  temperature_c DECIMAL(5,2) NOT NULL,
  ph DECIMAL(3,2) NOT NULL,
  battery_voltage_v DECIMAL(4,2) NOT NULL,
  INDEX idx_sensor_time (sensor_id, recorded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def parse_args():
    p = argparse.ArgumentParser(description="Inserir leituras em formato de onda a cada N segundos em MySQL.")
    p.add_argument("--host", default="localhost", help="DB host (default localhost)")
    p.add_argument("--port", type=int, default=3306, help="DB port (default 3306)")
    p.add_argument("--database", default="test", help="DB name (default test)")
    p.add_argument("--user", default="root", help="DB user (default root)")
    p.add_argument("--password", default="", help="DB password (default empty)")
    p.add_argument("--sensor-id", default="ground-01", help="Sensor ID (default ground-01)")
    p.add_argument("--interval", type=float, default=4.0, help="Intervalo entre inserts em segundos (default 4)")
    p.add_argument("--period", type=float, default=60.0, help="Período da onda em segundos (default 60)")
    p.add_argument("--create-table", action="store_true", help="Criar tabela se não existir")
    return p.parse_args()

def open_conn(args):
    return mysql.connector.connect(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
        autocommit=False
    )

def generate_values(elapsed_seconds, period):
    """
    Gera valores em forma de onda com ruído:
      - moisture_pct: baseline 40, amp 20 => varia ~[20,60]
      - temperature_c: baseline 18, amp 5 => varia ~[13,23]
      - ph: baseline 6.8, amp 0.3 => varia ~[6.5,7.1]
      - battery: baseline 3.7, queda lenta + pequena oscilação
    """
    angle = 2.0 * math.pi * (elapsed_seconds / period)

    noise_moisture = random.uniform(-0.5, 0.5)
    noise_temp = random.uniform(-0.1, 0.1)
    noise_ph = random.uniform(-0.02, 0.02)
    noise_batt = random.uniform(-0.005, 0.005)

    moisture = 40.0 + 20.0 * math.sin(angle) + noise_moisture
    moisture = clamp(round(moisture, 2), 0.0, 100.0)

    temperature = 18.0 + 5.0 * math.sin(angle + 0.5) + noise_temp
    temperature = clamp(round(temperature, 2), -50.0, 100.0)

    ph = 6.8 + 0.3 * math.sin(angle + 1.0) + noise_ph
    ph = clamp(round(ph, 2), 0.0, 14.0)

    battery_baseline = 3.70
    decay_per_hour = 0.02
    decay = (decay_per_hour / 3600.0) * elapsed_seconds
    battery = battery_baseline - decay + 0.03 * math.sin(angle + 2.0) + noise_batt
    battery = clamp(round(battery, 2), 0.0, 10.0)

    return moisture, temperature, ph, battery

def try_reconnect(args, attempts=3, delay=2.0):
    last_exc = None
    for i in range(attempts):
        try:
            conn = open_conn(args)
            return conn
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    raise last_exc

def main():
    args = parse_args()

    print(f"Conectando a {args.host}:{args.port} database={args.database} user={args.user}")
    start_time = time.time()

    try:
        conn = try_reconnect(args)
    except Exception as e:
        print("Erro ao conectar ao banco:", e)
        sys.exit(1)

    with closing(conn):
        cur = conn.cursor()
        if args.create_table:
            try:
                cur.execute(CREATE_TABLE_SQL)
                conn.commit()
                print("Tabela criada (ou já existia).")
            except Exception as e:
                print("Falha ao criar tabela:", e)
                return

        print(f"Iniciando loop de inserts a cada {args.interval} segundos. Sensor id = {args.sensor_id}")
        try:
            while True:
                elapsed = time.time() - start_time
                moisture, temperature, ph, battery = generate_values(elapsed, args.period)

                try:
                    cur.execute(INSERT_SQL, (
                        args.sensor_id,
                        moisture,
                        temperature,
                        ph,
                        battery
                    ))
                    conn.commit()
                except db_errors.OperationalError as oe:
                    print("Erro operacional no INSERT:", oe, "- tentando reconectar")
                    try:
                        conn.close()
                    except:
                        pass
                    try:
                        conn = try_reconnect(args)
                        cur = conn.cursor()
                        cur.execute(INSERT_SQL, (
                            args.sensor_id,
                            moisture,
                            temperature,
                            ph,
                            battery
                        ))
                        conn.commit()
                        print("Reconectado e inserido com sucesso.")
                    except Exception as e2:
                        print("Falha ao reconectar/inserir:", e2)
                        time.sleep(max(1.0, args.interval))
                        continue
                except Exception as e:
                    print("Erro no INSERT:", e)
                    time.sleep(max(1.0, args.interval))
                    continue

                ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print(f"[{ts}] inserted: sensor={args.sensor_id} moisture={moisture}% temp={temperature}C ph={ph} batt={battery}V")

                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nInterrompido pelo usuário. Saindo...")
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    main()