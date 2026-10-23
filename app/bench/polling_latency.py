"""Mide la latencia y el costo de una estrategia de polling por timestamp (actualizado_en).
Uso: python bench/polling_latency.py [N] [PAUSA_ENTRE_INSERTS_S] [INTERVALO_POLLING_S]"""
import os
import statistics
import sys
import threading
import time
import uuid

import psycopg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/pedidos_db")
N = int(sys.argv[1]) if len(sys.argv) > 1 else 100
GAP = float(sys.argv[2]) if len(sys.argv) > 2 else 0.2
INTERVALO = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0
N_DEL = min(10, N)

run = uuid.uuid4().hex[:8]
prefix = f"bench-{run}-"
t_sent, t_detect = {}, {}
consultas = {"total": 0, "con_cambios": 0}
borrados, tras_borrado = set(), set()
stop = threading.Event()
listo = threading.Event()


def poller():
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    marca = conn.execute("SELECT now()").fetchone()[0]
    listo.set()
    while not stop.is_set():
        filas = conn.execute(
            "SELECT cliente, actualizado_en FROM pedidos WHERE actualizado_en > %s ORDER BY actualizado_en",
            (marca,),
        ).fetchall()
        ahora = time.time()
        consultas["total"] += 1
        if filas:
            consultas["con_cambios"] += 1
            marca = filas[-1][1]
            for cliente, _ in filas:
                t_detect.setdefault(cliente, ahora)
        if borrados:
            tras_borrado.update(c for c, _ in filas if c in borrados)
        stop.wait(INTERVALO)
    conn.close()


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


hilo = threading.Thread(target=poller)
hilo.start()
listo.wait()
t_ini = time.time()
with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
    insertar = "INSERT INTO pedidos (cliente, producto, cantidad, total) VALUES (%s, 'bench', 1, 1.00)"
    print(f"Polling cada {INTERVALO} s. Enviando {N} INSERT (uno cada {GAP} s)...")
    for i in range(N):
        nombre = f"{prefix}{i}"
        t_sent[nombre] = time.time()
        conn.execute(insertar, (nombre,))
        time.sleep(GAP)

    nombres = [f"{prefix}{i}" for i in range(N)]
    esperar(lambda: all(k in t_detect for k in nombres), 3 * INTERVALO + 10)
    detectados = [k for k in nombres if k in t_detect]
    print(f"\nINSERT detectados: {len(detectados)} de {N}")
    if detectados:
        resumen("Latencia extremo a extremo (inicio del INSERT -> detección):",
                [(t_detect[k] - t_sent[k]) * 1000 for k in detectados])

    borrar = nombres[:N_DEL]
    borrados.update(borrar)
    for nombre in borrar:
        conn.execute("DELETE FROM pedidos WHERE cliente = %s", (nombre,))
    time.sleep(2 * INTERVALO + 1)
    conn.execute("DELETE FROM pedidos WHERE cliente LIKE %s", (prefix + "%",))

stop.set()
hilo.join()
duracion = time.time() - t_ini
print(f"\nDELETE detectados por polling: {len(tras_borrado)} de {N_DEL} "
      "(la consulta por timestamp no devuelve filas eliminadas)")
print(f"Consultas ejecutadas contra pedidos: {consultas['total']} en {duracion:.0f} s "
      f"(~{consultas['total'] / duracion * 60:.0f} por minuto); "
      f"con cambios: {consultas['con_cambios']}, vacías: {consultas['total'] - consultas['con_cambios']}")