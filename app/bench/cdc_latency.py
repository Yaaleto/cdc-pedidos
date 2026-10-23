"""Mide la latencia del flujo CDC: INSERT en PostgreSQL -> evento recibido desde Kafka.
Uso: python bench/cdc_latency.py [N] [PAUSA_ENTRE_INSERTS_S]"""
import json
import os
import statistics
import sys
import threading
import time
import uuid

import psycopg
from confluent_kafka import Consumer

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/pedidos_db")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "cdc.public.pedidos")
N = int(sys.argv[1]) if len(sys.argv) > 1 else 100
GAP = float(sys.argv[2]) if len(sys.argv) > 2 else 0.2
N_DEL = min(10, N)

run = uuid.uuid4().hex[:8]
prefix = f"bench-{run}-"
t_sent, t_recv, src_ts = {}, {}, {}
borrados_vistos = set()
stop = threading.Event()


def consumir():
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"bench-{run}", "auto.offset.reset": "earliest"})
    c.subscribe([TOPIC])
    while not stop.is_set():
        msg = c.poll(0.2)
        if msg is None or msg.error() or msg.value() is None:
            continue
        ev = json.loads(msg.value())
        ev = ev.get("payload", ev)
        ahora = time.time()
        op, before, after = ev.get("op"), ev.get("before"), ev.get("after")
        if op == "c" and after and after["cliente"].startswith(prefix):
            t_recv.setdefault(after["cliente"], ahora)
            src_ts.setdefault(after["cliente"], ev["source"]["ts_ms"])
        elif op == "d" and before and before["cliente"].startswith(prefix):
            borrados_vistos.add(before["cliente"])
    c.close()


def esperar(condicion, timeout):
    fin = time.time() + timeout
    while time.time() < fin:
        if condicion():
            return True
        time.sleep(0.05)
    return False


def resumen(nombre, xs):
    xs = sorted(xs)
    n = len(xs)
    p95 = xs[min(n - 1, int(0.95 * n))]
    print(f"{nombre}\n   n={n}  media={statistics.mean(xs):.1f} ms  mediana={statistics.median(xs):.1f} ms  "
          f"p95={p95:.1f} ms  max={xs[-1]:.1f} ms")


threading.Thread(target=consumir, daemon=True).start()
with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
    insertar = "INSERT INTO pedidos (cliente, producto, cantidad, total) VALUES (%s, 'bench', 1, 1.00)"

    # Calentamiento: espera a que el consumidor se ponga al día y el flujo esté activo
    conn.execute(insertar, (prefix + "warm",))
    if not esperar(lambda: prefix + "warm" in t_recv, 90):
        sys.exit("No llegó el evento de calentamiento. ¿Debezium está RUNNING?")

    print(f"Flujo listo. Enviando {N} INSERT (uno cada {GAP} s)...")
    for i in range(N):
        nombre = f"{prefix}{i}"
        t_sent[nombre] = time.time()
        conn.execute(insertar, (nombre,))
        time.sleep(GAP)

    nombres = [f"{prefix}{i}" for i in range(N)]
    esperar(lambda: all(k in t_recv for k in nombres), 30)
    recibidos = [k for k in nombres if k in t_recv]
    print(f"\nEventos INSERT recibidos: {len(recibidos)} de {N}")
    if recibidos:
        resumen("Latencia extremo a extremo (inicio del INSERT -> consumidor):",
                [(t_recv[k] - t_sent[k]) * 1000 for k in recibidos])
        resumen("Latencia desde el cambio en la base (source.ts_ms -> consumidor):",
                [t_recv[k] * 1000 - src_ts[k] for k in recibidos])

    borrar = nombres[:N_DEL]
    for nombre in borrar:
        conn.execute("DELETE FROM pedidos WHERE cliente = %s", (nombre,))
    esperar(lambda: all(b in borrados_vistos for b in borrar), 20)
    print(f"\nDELETE detectados por CDC: {len(borrados_vistos & set(borrar))} de {N_DEL}")
    print("Consultas del consumidor a la tabla pedidos: 0 (por diseño, solo lee de Kafka)")
    conn.execute("DELETE FROM pedidos WHERE cliente LIKE %s", (prefix + "%",))
stop.set()