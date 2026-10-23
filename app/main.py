import asyncio
import json
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal

import psycopg
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/pedidos_db")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "cdc.public.pedidos")

# Vista de consulta: copia de los pedidos construida SOLO con eventos de Kafka.
read_model: dict[int, dict] = {}


class Manager:
    """Mantiene los navegadores conectados por WebSocket."""

    def __init__(self):
        self.clients: set[WebSocket] = set()

    async def broadcast(self, data: dict):
        for ws in list(self.clients):
            try:
                await ws.send_json(data)
            except Exception:
                self.clients.discard(ws)


manager = Manager()


def handle_event(ev: dict, loop: asyncio.AbstractEventLoop):
    if "payload" in ev:  # formato antiguo con schema
        ev = ev["payload"]
    op = ev.get("op")
    before, after = ev.get("before"), ev.get("after")
    src = ev.get("source", {})

    if op in ("c", "r", "u") and after:
        read_model[after["id"]] = after
    elif op == "d" and before:
        read_model.pop(before["id"], None)
    else:
        return

    now_ms = int(time.time() * 1000)
    event = {
        "type": "cdc",
        "op": op,
        "before": before,
        "after": after,
        "lsn": src.get("lsn"),
        "latency_ms": now_ms - src.get("ts_ms", now_ms),  # commit en PostgreSQL -> llegada al consumidor
    }
    asyncio.run_coroutine_threadsafe(manager.broadcast(event), loop)


def consumer_loop(loop: asyncio.AbstractEventLoop):
    # group.id nuevo en cada arranque: la vista se reconstruye releyendo el tópico desde el inicio
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"vista-consulta-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    print("Consumidor CDC iniciado, tópico:", TOPIC, flush=True)
    while True:
        try:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print("Error Kafka:", msg.error(), flush=True)
                continue
            if msg.value() is None:
                continue
            handle_event(json.loads(msg.value()), loop)
        except Exception as e:
            print("Error procesando evento:", e, flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    threading.Thread(target=consumer_loop, args=(loop,), daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


class Pedido(BaseModel):
    cliente: str
    producto: str
    cantidad: int = Field(gt=0)
    total: Decimal
    estado: str = "pendiente"


def db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True)


@app.get("/")
def index():
    return FileResponse("static/index.html")


# ---------- Sistema transaccional (escribe en PostgreSQL) ----------
@app.get("/api/pedidos")
def listar():
    with db() as conn:
        return conn.execute("SELECT * FROM pedidos ORDER BY id").fetchall()


@app.post("/api/pedidos")
def crear(p: Pedido):
    with db() as conn:
        return conn.execute(
            "INSERT INTO pedidos (cliente, producto, cantidad, total, estado) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING *",
            (p.cliente, p.producto, p.cantidad, p.total, p.estado),
        ).fetchone()


@app.put("/api/pedidos/{pedido_id}")
def actualizar(pedido_id: int, p: Pedido):
    with db() as conn:
        row = conn.execute(
            "UPDATE pedidos SET cliente=%s, producto=%s, cantidad=%s, total=%s, estado=%s, "
            "actualizado_en=now() WHERE id=%s RETURNING *",
            (p.cliente, p.producto, p.cantidad, p.total, p.estado, pedido_id),
        ).fetchone()
    if not row:
        raise HTTPException(404, "Pedido no encontrado")
    return row


@app.delete("/api/pedidos/{pedido_id}")
def borrar(pedido_id: int):
    with db() as conn:
        row = conn.execute("DELETE FROM pedidos WHERE id=%s RETURNING id", (pedido_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Pedido no encontrado")
    return row


# ---------- Sistema de consulta (solo lee de Kafka) ----------
@app.get("/api/consulta")
def consulta():
    return sorted(read_model.values(), key=lambda r: r["id"])


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    manager.clients.add(ws)
    await ws.send_json({"type": "snapshot", "rows": sorted(read_model.values(), key=lambda r: r["id"])})
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.clients.discard(ws)