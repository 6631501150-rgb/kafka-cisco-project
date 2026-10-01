from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel
from kafka import KafkaConsumer, KafkaProducer
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from collections import OrderedDict
import os
import threading


app = FastAPI(
    title="Cisco Command Producer API",
    version="1.0.0"
)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cisco-commands")
KAFKA_RESULT_TOPIC = os.getenv("KAFKA_RESULT_TOPIC", "cisco-results")

FRONTEND_DIR = Path(__file__).parent / "frontend"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# Latest result per event_id, filled from the worker's result topic
MAX_RESULTS = 500
results = OrderedDict()
results_lock = threading.Lock()


def consume_results():
    consumer = KafkaConsumer(
        KAFKA_RESULT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    for message in consumer:
        result = message.value

        with results_lock:
            results.pop(result["event_id"], None)
            results[result["event_id"]] = result

            while len(results) > MAX_RESULTS:
                results.popitem(last=False)


threading.Thread(target=consume_results, daemon=True).start()


class CommandRequest(BaseModel):
    router_ip: str
    command: str


@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "Cisco Command Producer API"
    }


@app.get("/ui", include_in_schema=False)
def ui():
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "kafka": KAFKA_BOOTSTRAP,
        "topic": KAFKA_TOPIC,
        "result_topic": KAFKA_RESULT_TOPIC
    }


@app.post("/api/commands")
def send_command(request: CommandRequest):

    event = {
        "event_id": str(uuid.uuid4()),
        "router_ip": request.router_ip,
        "command": request.command,
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    future = producer.send(
        KAFKA_TOPIC,
        value=event
    )

    metadata = future.get(timeout=10)

    return {
        "success": True,
        "message": "Command sent to Kafka",
        "event": event,
        "kafka": {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset
        }
    }


@app.get("/api/commands/{event_id}")
def get_command_result(event_id: str):

    with results_lock:
        result = results.get(event_id)

    if result is None:
        return {
            "event_id": event_id,
            "status": "pending"
        }

    return result
