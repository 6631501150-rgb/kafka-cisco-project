from kafka import KafkaProducer
import json
import uuid
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "cisco-commands"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def send_cisco_command(command, router_ip):

    event = {
        "event_id": str(uuid.uuid4()),
        "router_ip": router_ip,
        "command": command,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    future = producer.send(TOPIC, value=event)
    metadata = future.get(timeout=10)

    print("Message sent")
    print(f"Topic     : {metadata.topic}")
    print(f"Partition : {metadata.partition}")
    print(f"Offset    : {metadata.offset}")
    print(f"Event     : {event}")


if __name__ == "__main__":

    send_cisco_command(
        command="show ip interface brief",
        router_ip="192.168.6.129",
    )

    producer.flush()
