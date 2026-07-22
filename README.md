# Live Location Tracker (Kafka)

A minimal but complete pipeline for streaming and visualizing live location
updates through Kafka.

```
[producer.py]  --(Kafka topic: device-locations)-->  [consumer_ws.py]  --(WebSocket)-->  [frontend/index.html]
 simulated GPS         Kafka broker                    bridges Kafka to             live map in the browser
 devices                (docker)                       the browser
```

- **producer.py** simulates a small fleet of moving devices (vehicles) and
  publishes a JSON location event for each one, every second, to the Kafka
  topic `device-locations`. In production, this role is played by your real
  devices/apps/vehicles publishing directly to Kafka (or through an ingest
  gateway), not by this script.
- **Kafka** durably buffers and distributes the stream. This is the part
  that lets you add more consumers later (e.g. a geofencing service, an
  analytics job, a storage sink) without touching the producers.
- **consumer_ws.py** subscribes to the topic and rebroadcasts every message
  to any connected browser over a WebSocket, since browsers can't speak
  Kafka's wire protocol directly.
- **frontend/index.html** is a live map (Leaflet + CartoDB dark tiles) that
  connects to the WebSocket bridge and plots each device's position, trail,
  speed, and heading in real time.

## 1. Start Kafka

Requires Docker.

```bash
docker compose up -d
```

This runs a single-node Kafka broker in KRaft mode (no ZooKeeper needed),
reachable at `localhost:29092` from your host machine.

Check it's healthy:

```bash
docker compose ps
```

## 2. Install Python dependencies

```bash
python -m venv venv
source venv/bin/activate      # on Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Run the consumer bridge

```bash
python consumer_ws.py
```

You should see:
```
Kafka consumer listening on topic 'device-locations'
WebSocket bridge live at ws://localhost:8765
```

## 4. Run the producer

In a second terminal:

```bash
python producer.py
```

You'll see log lines as each simulated vehicle reports its position.

## 5. Open the map

Just open `frontend/index.html` directly in a browser (double-click it, or
`open frontend/index.html` / `xdg-open frontend/index.html`). It connects to
`ws://localhost:8765` and starts drawing markers and trails as events arrive.

## Customizing

- **Change the simulated fleet's home city**: edit `base_lat, base_lng` in
  `producer.py`.
- **Number of devices / how often they report**: `NUM_DEVICES` and
  `TICK_SECONDS` in `producer.py`.
- **Real devices instead of simulation**: point your real GPS source at the
  same Kafka topic (`device-locations`) using the same JSON shape:
  ```json
  {
    "device_id": "vehicle-1",
    "lat": 28.6139,
    "lng": 77.2090,
    "heading": 90.0,
    "speed_kmh": 42.5,
    "timestamp": "2026-07-22T10:15:00+00:00"
  }
  ```
  You don't need to change the consumer or frontend at all.
- **Scaling consumers**: `consumer_ws.py` uses Kafka consumer group
  `location-ws-bridge`. You can run multiple instances behind a load
  balancer, or add entirely separate consumers (e.g. one that writes to a
  database for history, one that checks geofences) reading the same topic
  independently.

## Troubleshooting

- **Producer/consumer can't connect to Kafka**: make sure `docker compose
  ps` shows the broker healthy, and that you're using port `29092` (the
  host-facing listener), not `9092` (used internally between Docker
  containers).
- **Map shows "reconnecting…" forever**: make sure `consumer_ws.py` is
  running and nothing else is bound to port `8765`.
- **No devices appear**: make sure `producer.py` is running — the map only
  shows devices once at least one location event has arrived.
