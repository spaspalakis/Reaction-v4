import argparse
import json
import os
import sys

from confluent_kafka import Consumer, KafkaError

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.json")


def _default_broker():
    try:
        with open(_CONFIG_PATH, "r") as f:
            cfg = json.load(f)
        return cfg.get("broker_vpn") or cfg.get("broker") or "10.147.0.1:9094"
    except (OSError, json.JSONDecodeError):
        return "10.147.0.1:9094"


def _topic_from_interactive():
    print("Select topic to listen to:")
    print("1. UAV_Telemetry")
    print("2. ObjectDetection")
    choice = input("Enter 1 or 2: ").strip()
    if choice == "1":
        return "UAV_Telemetry"
    if choice == "2":
        return "ObjectDetection"
    print("Invalid choice. Exiting.")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Minimal Kafka consumer for test topics (non-interactive: pass --topic)."
    )
    parser.add_argument(
        "--topic",
        choices=("UAV_Telemetry", "ObjectDetection"),
        help="Topic to subscribe (required when stdin is not a TTY, e.g. plain docker run).",
    )
    parser.add_argument(
        "--broker",
        default=os.environ.get("KAFKA_BROKER", _default_broker()),
        help="Kafka bootstrap servers (default: broker_vpn or broker from functions/config.json, or env KAFKA_BROKER).",
    )
    parser.add_argument(
        "--group-id",
        default="ode-consumer",
        help="Consumer group id (use a new name to read from latest position with a fresh group).",
    )
    parser.add_argument(
        "--from-earliest",
        action="store_true",
        help="Read from beginning of topic if no committed offset (default is latest: only new messages after subscribe).",
    )
    args = parser.parse_args()

    if args.topic:
        topic_in = args.topic
    elif sys.stdin.isatty():
        topic_in = _topic_from_interactive()
    else:
        parser.error(
            "stdin is not a TTY (e.g. docker without -it). "
            "Use: --topic UAV_Telemetry or --topic ObjectDetection"
        )

    conf = {
        "bootstrap.servers": args.broker,
        "security.protocol": "PLAINTEXT",
        "group.id": args.group_id,
        "auto.offset.reset": "earliest" if args.from_earliest else "latest",
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic_in])

    print(f"\nListening on topic: {topic_in} (broker: {args.broker})")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Error: {msg.error()}")
                break

            print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
