from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import uuid
from datetime import datetime, timezone


app = FastAPI(
    title="Cisco Command Producer API",
    version="1.0.0"
)


KAFKA_BOOTSTRAP = "localhost:29092"
KAFKA_TOPIC = "cisco-commands"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


class CommandRequest(BaseModel):
    router_ip: str
    command: str


@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "Cisco Command Producer API"
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "kafka": KAFKA_BOOTSTRAP,
        "topic": KAFKA_TOPIC
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
