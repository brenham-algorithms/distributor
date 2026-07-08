# distributor

Market data feed distributor. Subscribes to tick data sources and publishes to Redis for fan-out to multiple consumers.

## Architecture

```
Tick Sources                     Redis                    Consumers
─────────────                    ─────                    ─────────
ProjectX (SignalR)  ──→  ticks:{contract_id}  ──→  Farmer instances
gRPC (future)       ──→  ticks:{contract_id}  ──→  QuestDB tick writer
WebSocket (future)  ──→  ticks:{contract_id}  ──→  Delta monitor
```

Each tick source is a thin adapter that publishes a common tick message format to Redis. Consumers subscribe to the channels they care about. The tick sources and consumers are fully decoupled — adding a new source or consumer requires no changes to existing code.

## Tick Message Format

```json
{
  "symbolId": "F.US.MNQ",
  "price": 29508.75,
  "timestamp": "2026-07-07T13:35:48.144+00:00",
  "type": 1,
  "volume": 1,
  "contractId": "CON.F.US.MNQ.U26"
}
```

`type`: `0` = buy aggressor (lifted the ask), `1` = sell aggressor (hit the bid).

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install setuptools
pip install -e .
```

Requires Python 3.12+ and a running Redis instance.

## Usage

### ProjectX SignalR Subscriber

```bash
python entrypoints/signalr.py \
  --base-url https://api.topstepx.com \
  --market-hub-url https://rtc.topstepx.com/hubs/market \
  --username your_username \
  --api-key your_api_key \
  --contracts CON.F.US.MNQ.U26 CON.F.US.MES.U26
```

### Subscribing to Ticks (Consumer Example)

```python
import json
import redis

r = redis.Redis(host="localhost", port=6379, db=0)
pubsub = r.pubsub()
pubsub.subscribe("ticks:CON.F.US.MNQ.U26")

for message in pubsub.listen():
    if message["type"] == "message":
        tick = json.loads(message["data"])
        print(f"{tick['price']} {tick['volume']} {'BUY' if tick['type'] == 0 else 'SELL'}")
```

## Project Structure

```
entrypoints/
    signalr.py              # ProjectX SignalR → Redis
subscribers/
    projectx_subscriber.py  # SignalR connection and trade handling
```

## Dependencies

- [projectx-python](https://github.com/brenham-algorithms/projectx-python) — shared ProjectX API client
- [redis](https://github.com/redis/redis-py) — Redis client
- [signalrcore](https://github.com/mandrewcito/signalrcore) — SignalR client

## Related Repos

- [farmer](https://github.com/brenham-algorithms/farmer) — trading strategy engine (consumes ticks from Redis)
- [projectx-python](https://github.com/brenham-algorithms/projectx-python) — shared ProjectX API client
