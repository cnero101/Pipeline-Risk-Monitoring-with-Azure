"""
sensorsim_azure.py
Streams synthetic sensor telemetry for 5 Alberta pipelines to Azure Event Hubs.
Sends 5 readings per second (one per pipeline) in batches.

Usage:
    python sensorsim_azure.py

Environment variables (set in .env or export directly):
    EVENT_HUB_CONN_STR  — Event Hubs namespace connection string
    EVENT_HUB_NAME      — Event Hub name (default: pipeline-sensor-hub)
"""

import json
import os
import time
import random
from datetime import datetime

from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

load_dotenv()

EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONN_STR")
EVENT_HUB_NAME     = os.getenv("EVENT_HUB_NAME", "pipeline-sensor-hub")

PIPES = ["P-101", "P-102", "P-103", "P-104", "P-105"]


def generate_reading(pipe_id: str) -> dict:
    """Generate a synthetic sensor reading for a given pipeline."""
    return {
        "pipe_id":           pipe_id,
        "timestamp":         datetime.utcnow().isoformat(),
        "pressure_MPa":      round(random.uniform(5.0, 6.5), 3),
        "temperature_C":     round(random.uniform(20.0, 60.0), 2),
        "flow_rate_percent": round(random.uniform(-10.0, 10.0), 2),
    }


def main():
    if not EVENT_HUB_CONN_STR:
        raise ValueError(
            "EVENT_HUB_CONN_STR is not set. "
            "Copy .env.example to .env and fill in your connection string."
        )

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONN_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    print(f"Streaming to Azure Event Hubs: {EVENT_HUB_NAME}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            batch = producer.create_batch()
            for pipe_id in PIPES:
                reading = generate_reading(pipe_id)
                batch.add(EventData(json.dumps(reading)))
            producer.send_batch(batch)
            print(f"[{datetime.utcnow().isoformat()}] Sent {len(PIPES)} readings")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
