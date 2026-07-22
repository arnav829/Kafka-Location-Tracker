"""
consumer_ws.py — consumes location events from Kafka and rebroadcasts them
over a WebSocket so the browser-based map (frontend/index.html) can show
them live.

Browsers can't speak the Kafka wire protocol directly, so this bridge
does the one job of turning "new Kafka message" into "push to every
connected websocket client".

Usage:
    python consumer_ws.py
"""

import asyncio
import json
import threading

import websockets
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = ["localhost:29092"]
TOPIC = "device-locations"
WS_HOST = "localhost"
WS_PORT = 8765

connected_clients = set()
main_loop = None  # set once the asyncio loop is running


def kafka_listener():
    """Runs in a background thread: blocking Kafka consume loop."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="location-ws-bridge",
    )
    print(f"Kafka consumer listening on topic '{TOPIC}'")

    for message in consumer:
        event = message.value
        if main_loop is not None:
            asyncio.run_coroutine_threadsafe(broadcast(event), main_loop)


async def broadcast(event: dict):
    if not connected_clients:
        return
    payload = json.dumps(event)
    dead = set()
    for ws in connected_clients:
        try:
            await ws.send(payload)
        except websockets.exceptions.ConnectionClosed:
            dead.add(ws)
    connected_clients.difference_update(dead)


async def handler(websocket):
    connected_clients.add(websocket)
    print(f"Client connected ({len(connected_clients)} total)")
    try:
        async for _ in websocket:
            pass  # this bridge is one-way: server -> browser
    finally:
        connected_clients.discard(websocket)
        print(f"Client disconnected ({len(connected_clients)} total)")


async def main():
    global main_loop
    main_loop = asyncio.get_running_loop()

    threading.Thread(target=kafka_listener, daemon=True).start()

    async with websockets.serve(handler, WS_HOST, WS_PORT):
        print(f"WebSocket bridge live at ws://{WS_HOST}:{WS_PORT}")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
