from kafka import KafkaConsumer
import paramiko
import json

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "cisco-commands"

ROUTER_USERNAME = "admin"
ROUTER_PASSWORD = "cisco"


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id="cisco-worker-test-v2",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)


def execute_cisco_command(router_ip, command):
    transport = None

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
        print(f"SSH ERROR: {repr(e)}")

    finally:
        if transport:
            transport.close()

        print("Connection closed")


print("================================")
print("Cisco Kafka Worker Started")
print("================================")
print(f"Kafka : {KAFKA_BOOTSTRAP}")
print(f"Topic : {TOPIC}")
print("Waiting for Kafka events...")


for message in consumer:

    event = message.value

    print("\n================================")
    print("Kafka Event Received")
    print("================================")

    print(f"Event ID : {event['event_id']}")
    print(f"Router   : {event['router_ip']}")
    print(f"Command  : {event['command']}")

    execute_cisco_command(
        event["router_ip"],
        event["command"]
    )
