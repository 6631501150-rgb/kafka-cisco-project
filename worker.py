from kafka import KafkaConsumer, KafkaProducer
import paramiko
import json
import os
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "cisco-commands")
RESULT_TOPIC = os.getenv("KAFKA_RESULT_TOPIC", "cisco-results")
GROUP_ID = os.getenv("WORKER_GROUP_ID", "cisco-worker-test-v2")

ROUTER_USERNAME = os.getenv("ROUTER_USERNAME", "admin")
ROUTER_PASSWORD = os.getenv("ROUTER_PASSWORD", "cisco")


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def publish_result(event, status, output="", error=""):
    producer.send(
        RESULT_TOPIC,
        value={
            "event_id": event["event_id"],
            "router_ip": event["router_ip"],
            "command": event["command"],
            "status": status,
            "output": output,
            "error": error,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    producer.flush()


def execute_cisco_command(router_ip, command):
    transport = None
    output = ""
    error = ""

    try:
        print(f"\nConnecting to {router_ip}...")

        transport = paramiko.Transport(
            (router_ip, 22)
        )

        opts = transport.get_security_options()

        # Cisco SSH compatibility
        opts.kex = [
            "diffie-hellman-group14-sha1",
        ]

        opts.ciphers = [
            "aes128-cbc",
        ]

        print("KEX    : diffie-hellman-group14-sha1")
        print("Cipher : aes128-cbc")

        transport.connect(
            username=ROUTER_USERNAME,
            password=ROUTER_PASSWORD,
        )

        print("SSH Connected!")

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(
            paramiko.AutoAddPolicy()
        )

        ssh._transport = transport

        print(f"Executing command: {command}")

        stdin, stdout, stderr = ssh.exec_command(
            command
        )

        output = stdout.read().decode(
            "utf-8",
            errors="replace"
        )

        error = stderr.read().decode(
            "utf-8",
            errors="replace"
        )

        print("\n========== OUTPUT ==========")
        print(output)

        if error:
            print("\n========== ERROR ==========")
            print(error)

        ssh.close()

    except Exception as e:
        error = f"SSH ERROR: {repr(e)}"
        print(error)
        return False, output, error

    finally:
        if transport:
            transport.close()

        print("Connection closed")

    return not error, output, error


print("================================")
print("Cisco Kafka Worker Started")
print("================================")
print(f"Kafka : {KAFKA_BOOTSTRAP}")
print(f"Topic : {TOPIC}")
print(f"Result: {RESULT_TOPIC}")
print("Waiting for Kafka events...")


for message in consumer:

    event = message.value

    print("\n================================")
    print("Kafka Event Received")
    print("================================")

    print(f"Event ID : {event['event_id']}")
    print(f"Router   : {event['router_ip']}")
    print(f"Command  : {event['command']}")

    publish_result(event, "running")

    success, output, error = execute_cisco_command(
        event["router_ip"],
        event["command"]
    )

    publish_result(event, "success" if success else "failed", output, error)
