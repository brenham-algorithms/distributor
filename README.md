# distributor

Market data and user event distributor. Subscribes to ProjectX market hub (tick data) and user hub (orders, positions, accounts) via SignalR, and publishes all events to Redis for fan-out to multiple consumers.

## Architecture

```
ProjectX SignalR                   Redis                         Consumers
────────────────                   ─────                         ─────────
Market Hub ──→ ticks:{contract_id}              ──→ Farmer instances (RedisTicker)
User Hub   ──→ user:orders                      ──→ Order managers (position sync)
           ──→ user:positions                   ──→ Order managers (fill confirmation)
           ──→ user:accounts                    ──→ Balance monitoring
           ──→ user:trades                      ──→ Trade logging
```

A single distributor process maintains one market hub connection and one user hub connection. All downstream consumers — multiple farmer instances, order managers, monitors — read from Redis. This works around the limitation of one SignalR connection at a time.

## Redis Channels

### Market Data

Channel: `ticks:{contract_id}`

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

`type`: `0` = buy aggressor, `1` = sell aggressor.

### User Events

Channel: `user:orders` — order lifecycle events (submitted, filled, cancelled)

Channel: `user:positions` — position opened/closed/updated events

Channel: `user:accounts` — account balance updates

Channel: `user:trades` — trade execution events

All user event payloads are the raw JSON from the ProjectX user hub.

## Configuration

```yaml
projectx:
  base_url: "https://api.topstepx.com"
  market_hub_base_url: "https://rtc.topstepx.com/hubs/market"
  user_hub_base_url: "https://rtc.topstepx.com/hubs/user"
  username: "your_username"
  api_key: "your_api_key"
  contract_ids:
    - "CON.F.US.MNQ.U26"
    - "CON.F.US.CLE.U26"
  account_ids:
    - 12345
  redis_host: "localhost"
  redis_port: 6379
```

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install setuptools
pip install -e .
```

Requires Python 3.12+ and a running Redis instance:

```bash
docker run -d --name redis -p 6379:6379 redis:latest
```

## Usage

```bash
python entrypoints/projectx.py --config config.yaml --level info
```

Verify ticks are flowing:

```bash
docker exec -it redis redis-cli SUBSCRIBE "ticks:CON.F.US.MNQ.U26"
```

Verify user events are flowing:

```bash
docker exec -it redis redis-cli SUBSCRIBE "user:orders" "user:positions"
```

## Project Structure

```
entrypoints/
    projectx.py     # ProjectX SignalR (market + user hub) → Redis
```

## Dependencies

- [projectx-python](https://github.com/brenham-algorithms/projectx-python) — shared ProjectX API client
- [redis](https://github.com/redis/redis-py) — Redis client
- [signalrcore](https://github.com/mandrewcito/signalrcore) — SignalR client

## Related Repos

- [farmer](https://github.com/brenham-algorithms/farmer) — trading strategy engine (consumes ticks and user events from Redis)
- [projectx-python](https://github.com/brenham-algorithms/projectx-python) — shared ProjectX API client
