"""
Test consumer that listens to ObjectDetection topic and decodes
base64 images from detection messages.

Usage:
    python3 consumer_images.py
    python3 consumer_images.py --broker 10.147.0.1:9094   # VPN broker
"""
import json
import base64
import os
import argparse
from confluent_kafka import Consumer, KafkaException

DEFAULT_BROKER = "apps.edutel.uniwa.gr:9092"
TOPIC = "ObjectDetection"
OUTPUT_DIR = "decoded_frames"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conf = {
        'bootstrap.servers': args.broker,
        'security.protocol': 'PLAINTEXT',
        'group.id': 'image-consumer-test',
        'auto.offset.reset': 'latest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    print(f"Listening on {TOPIC} (broker: {args.broker})...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                print(f"Error: {msg.error()}")
                break

            payload = json.loads(msg.value().decode('utf-8'))
            detection_list = (payload
                              .get("records", [{}])[0]
                              .get("value", {})
                              .get("header", {})
                              .get("body", {})
                              .get("detection_list", []))

            for frame_data in detection_list:
                frame_id = frame_data.get("frameID", "unknown")
                image_b64 = frame_data.get("imageData", "")

                if image_b64:
                    img_bytes = base64.b64decode(image_b64)
                    out_path = os.path.join(OUTPUT_DIR, f"frame_{frame_id:04d}.jpg")
                    with open(out_path, "wb") as f:
                        f.write(img_bytes)
                    print(f"Saved {out_path} ({len(img_bytes)} bytes)")
                else:
                    print(f"Frame {frame_id}: no imageData")

                det_count = len(frame_data.get("detections", []))
                geo = frame_data.get("GeoLocation", {})
                print(f"  -> {det_count} detections | GeoLocation: {geo}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
