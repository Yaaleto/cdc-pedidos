# MVP: Change Data Capture con PostgreSQL, Debezium y Kafka

#### Proyecto de laboratorio de Sistemas de Bases de Datos 2 — CUNORI, USAC (2026).  
#### Autor: Yahir Alejandro Zabaleta Torres

Este MVP captura operaciones `INSERT`, `UPDATE` y `DELETE` de la tabla `pedidos` en PostgreSQL mediante decodificación lógica y las publica en Apache Kafka. Un consumidor en FastAPI mantiene una vista de consulta independiente, sin consultar periódicamente la tabla de origen para detectar cambios.

```text
PostgreSQL
    ↓
WAL + decodificación lógica
    ↓
Debezium (replication slot)
    ↓
Apache Kafka
    ↓
Consumidor FastAPI
    ↓
Vista de consulta (WebSocket)
```

## Requisitos

- Docker Desktop con WSL 2 (Windows), o Docker Engine con Docker Compose v2 (Linux/macOS).
- Aproximadamente 4 GB de RAM disponibles para Docker (recomendación para la prueba local).
- Puertos disponibles: `5433` (PostgreSQL), `8000` (aplicación), `8083` (Kafka Connect), `8090` (Kafka UI) y `29092` (Kafka externo).

Los comandos principales están escritos para **PowerShell**. En Linux/macOS, cambia las rutas cuando sea necesario y utiliza `curl` en lugar de `curl.exe`.

> Si un puerto está ocupado, revisa `ports` en `docker-compose.yml`. Para Kafka también debes revisar los *advertised listeners* si modificas el puerto externo.

## Estructura del proyecto

```text
cdc-pedidos/
├── docker-compose.yml
├── db/
│   └── init.sql
├── connector/
│   └── pedidos-connector.json
└── app/
    ├── main.py
    ├── static/
    │   └── index.html
    └── bench/
        ├── cdc_latency.py
        └── polling_latency.py
```

- `docker-compose.yml`: servicios de PostgreSQL, Kafka, Kafka Connect/Debezium, Kafka UI y aplicación.
- `db/init.sql`: tabla `pedidos` y configuración `REPLICA IDENTITY FULL`.
- `connector/pedidos-connector.json`: configuración del conector de Debezium.
- `app/main.py`: API transaccional, consumidor Kafka y comunicación WebSocket.
- `app/static/index.html`: interfaz de las dos vistas.
- `app/bench/`: scripts de medición CDC y polling.

## Puesta en marcha

Ejecuta los comandos desde la carpeta raíz `cdc-pedidos`.

### 1. Levantar los servicios

```powershell
docker compose up -d --build
docker compose ps
```

Comprueba que los cinco servicios estén en ejecución. La primera descarga de imágenes puede tardar y un contenedor `running` no garantiza que su servicio ya esté listo.

Para comprobar que Kafka Connect responde:

```powershell
curl.exe http://localhost:8083/connectors
```

### 2. Verificar la tabla de PostgreSQL

```powershell
docker exec -it pg_pedidos psql -U postgres -d pedidos_db -c "\d pedidos"
```

La tabla `pedidos` debe existir antes de registrar el conector.

### 3. Registrar el conector Debezium

En PowerShell:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" `
  -Body (Get-Content ".\connector\pedidos-connector.json" -Raw)

curl.exe http://localhost:8083/connectors/pedidos-connector/status
```

En Linux/macOS, el registro puede realizarse con:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @connector/pedidos-connector.json \
  http://localhost:8083/connectors

curl http://localhost:8083/connectors/pedidos-connector/status
```

Comprueba que el conector y su tarea muestren `"state": "RUNNING"`. Si Connect todavía no responde, vuelve a comprobar su disponibilidad. Si el conector ya existe, no lo registres por segunda vez mediante `POST`.

### 4. Abrir el MVP

- **Aplicación:** http://localhost:8000
- **Kafka UI:** http://localhost:8090 → **Topics** → `cdc.public.pedidos` → **Messages**

## Demostración: INSERT, UPDATE y DELETE

1. **INSERT:** crea un pedido en la vista transaccional (izquierda). Comprueba que aparezca en la vista de consulta (derecha).
2. **UPDATE:** cambia su estado y observa el evento con valores anteriores y nuevos.
3. **DELETE:** elimina el pedido y verifica que desaparezca de la vista de consulta.

El backend recibe los eventos desde Kafka y actualiza la vista por WebSocket, sin consultar periódicamente PostgreSQL para detectar cambios.

Para revisar el replication slot:

```powershell
docker exec -it pg_pedidos psql -U postgres -d pedidos_db -x -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, pg_current_wal_lsn() AS wal_actual, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS pendiente_de_confirmar, wal_status FROM pg_replication_slots;"
```

La diferencia entre `wal_actual` y `confirmed_flush_lsn` expresa un **atraso en posiciones del WAL**; no es una medida directa del espacio físico retenido en disco.

## Prueba de interrupción

1. Detén Kafka Connect/Debezium:

   ```powershell
   docker compose stop connect
   ```

2. Realiza cambios desde la vista transaccional; por ejemplo, tres `INSERT`, un `UPDATE` y un `DELETE`. La vista de consulta no recibirá eventos nuevos mientras el conector esté detenido.
3. Ejecuta la consulta del replication slot mostrada arriba. Comprueba `active = f` y compara `confirmed_flush_lsn` con `wal_actual`.
4. Reinicia Connect y revisa el estado del conector:

   ```powershell
   docker compose start connect
   curl.exe http://localhost:8083/connectors/pedidos-connector/status
   ```

5. Verifica en el log de la aplicación que lleguen los cambios pendientes. Repite la consulta del slot y comprueba si `active` vuelve a `t` y `confirmed_flush_lsn` avanza.

Los offsets del conector pueden inspeccionarse en Kafka UI → **Topics** → `connect_offsets` (habilita **Show internal topics**).

En la prueba documentada se recibieron los cinco cambios pendientes, sin duplicados observados. Esto no garantiza ausencia de duplicados ante fallos inesperados.

## Mediciones: CDC frente a polling

Los scripts se ejecutan dentro del contenedor `app`. Primero realiza una corrida de calentamiento cuyos resultados no se incluyen en la comparación:

```powershell
docker compose exec app python bench/cdc_latency.py 20 0.2
```

Después ejecuta las mediciones:

```powershell
docker compose exec app python bench/cdc_latency.py 100 0.2
docker compose exec app python bench/polling_latency.py 100 0.2 1
docker compose exec app python bench/polling_latency.py 100 0.2 5
docker compose exec app python bench/polling_latency.py 100 0.2 10
```

Los argumentos indican la **cantidad de inserciones**, la **pausa entre inserciones (segundos)** y, para polling, el **intervalo de consulta (segundos)**. Los scripts generan pedidos de prueba `bench-...` y los eliminan al finalizar; ejecútalos únicamente en un entorno de demostración.

Los resultados, las capturas y las limitaciones de estas mediciones están documentados en la nota **Resultados de latencia** del baúl de Obsidian.

## Problemas frecuentes

| Problema | Qué revisar |
|---|---|
| `port is already allocated` | Otro programa está usando el puerto. Revisa `ports` y, si se trata de Kafka, los *advertised listeners*. |
| Error al registrar el conector (`createRequest is null`) | Comprueba que `connector/pedidos-connector.json` tenga contenido JSON válido. |
| Tarea `FAILED` con `No table filters found` | Comprueba que la tabla `pedidos` exista y coincida con `table.include.list`. Si la base se inicializó incorrectamente, revisa `init.sql` antes de recrearla. |
| `Connection refused` en la aplicación | Comprueba que Kafka esté disponible; si el problema persiste, revisa los logs y reinicia la aplicación. |
| La vista de consulta no recibe eventos | Verifica el estado de la tarea de Debezium, la existencia del tópico y los logs del consumidor. |
| El conector ya existe | No repitas el `POST`; consulta su estado o actualiza la configuración con el método correspondiente. |

Para revisar los logs:

```powershell
docker compose logs --tail=100 connect
docker compose logs --tail=100 kafka
docker compose logs --tail=100 app
```

## Detener y limpiar

Detener y eliminar los contenedores de este proyecto:

```powershell
docker compose down
```

**Atención:** el siguiente comando elimina también los volúmenes del proyecto y puede borrar los datos de PostgreSQL y Kafka:

```powershell
docker compose down -v
```

Utilízalo solamente cuando quieras reiniciar la demostración desde cero y no necesites conservar los datos.

## Versiones y créditos

- PostgreSQL 16.
- Apache Kafka 3.8.0, modo KRaft (sin ZooKeeper).
- Debezium 3.0 sobre Kafka Connect.
- Kafka UI v0.7.2.
- Python 3.12, FastAPI, psycopg y confluent-kafka.

Las credenciales de ejemplo (`postgres/postgres`) son **solo para uso local** y no deben utilizarse en producción.

La implementación del MVP es propia. Se reconocen las herramientas y bibliotecas de terceros utilizadas.