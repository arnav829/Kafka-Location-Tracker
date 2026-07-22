"""
producer.py — simulates moving GPS devices and streams their location
updates into a Kafka topic as JSON messages.

In a real system, this file would not exist — real devices (phones,
vehicles, delivery bikes) would each publish their own location events
straight to Kafka. This script stands in for that fleet so you can see
the whole pipeline work end to end.

Usage:
    python producer.py
"""

import json
import math
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ["localhost:29092"]
TOPIC = "device-locations"
NUM_DEVICES = 15
TICK_SECONDS = 1.0  # how often each device reports its location


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(3, 8, 0),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=50,
    )


class SimulatedDevice:
    """A device that drives in a slowly-drifting circle around a home point,
    so the map shows continuous, realistic-looking movement."""

    def __init__(self, device_id: str, center_lat: float, center_lng: float):
        self.device_id = device_id
        self.center_lat = center_lat
        self.center_lng = center_lng
        self.radius_deg = random.uniform(0.01, 0.03)
        self.angle = random.uniform(0, 2 * math.pi)
        self.angular_speed = random.uniform(0.02, 0.06) * random.choice([-1, 1])
        self.speed_kmh = random.uniform(20, 60)

    def step(self) -> dict:
        self.angle += self.angular_speed
        lat = self.center_lat + self.radius_deg * math.sin(self.angle)
        lng = self.center_lng + self.radius_deg * math.cos(self.angle)
        # heading in degrees, purely derived from motion direction
        heading = (math.degrees(self.angle) + 90) % 360
        return {
            "device_id": self.device_id,
            "lat": round(lat, 6),
            "lng": round(lng, 6),
            "heading": round(heading, 1),
            "speed_kmh": round(self.speed_kmh + random.uniform(-3, 3), 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def main():
    producer = make_producer()
    print(f"Connected. Publishing to topic '{TOPIC}' on {KAFKA_BOOTSTRAP_SERVERS}")

    # Base fleet around a city center (New Delhi as a default; change as you like)
    base_lat, base_lng = 28.6139, 77.2090
    devices = [
        SimulatedDevice(
            device_id=f"vehicle-{i+1}",
            center_lat=base_lat + random.uniform(-0.05, 0.05),
            center_lng=base_lng + random.uniform(-0.05, 0.05),
        )
        for i in range(NUM_DEVICES)
    ]

    try:
        while True:
            for device in devices:
                event = device.step()
                producer.send(TOPIC, key=event["device_id"], value=event)
                print(f"  -> {event['device_id']} @ ({event['lat']}, {event['lng']})")
            producer.flush()
            time.sleep(TICK_SECONDS)
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
