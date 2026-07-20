# webhook-delivery

Accepts webhook events over HTTP and delivers them to customer endpoints. Kafka sits
between accept and deliver, so accepting is fast and durable while delivery is async —
with retries, exponential backoff, a dead-letter queue, per-endpoint ordering,
idempotency, concurrency limits, and per-host circuit breaking.

Built to learn Go and distributed-systems patterns. Single-broker local setup; see [Notes](#notes).

## Architecture

```
POST /events ─► API ─► events ─► delivery workers ─► POST to endpoint
                                       │ ok   → commit
                                       └ fail → retries ─► retry worker ─► waits, redelivers
                                                              │ exhausted / bad data → dead-letter
```

- API validates and publishes to `events` (acks=all), returns 202.
- Delivery workers: a consumer group; concurrent, per-key ordered, with a per-host circuit
  breaker and per-host + global concurrency caps.
- Retry worker: reads `retries`, waits until each message is due, redelivers.
- dead-letter: exhausted retries and unparseable messages.

## Run (Docker)

```sh
make up       # build + start kafka, api, 3 workers, retry-worker, receiver
make ps
make logs
make down     # stop (make reset also wipes Kafka data)
```

Send one — the endpoint host is the receiver's service name, since delivery runs inside
the Docker network:

```sh
curl -sXPOST localhost:8000/events \
  -d '{"endpointURL":"http://webhook-accept-api:8080/webhook/demo","payload":{"hello":"world"}}'
curl -s localhost:8080/store
```

Manual build/run without compose: [docs/docker.md](docs/docker.md).

## Run locally (go run)

Kafka in Docker, services on your machine:

```sh
make infra              # Kafka + topics only
make run-api            # run-worker / run-retry-worker / run-receiver in other terminals
make tester             # load at localhost:8000
```

Here services use `localhost:9092` and the bundled tester works as-is.

## Config

Env vars, all with defaults — see [`.env.example`](.env.example).

## Layout

```
cmd/
  api/                 HTTP API (accepts events)
  worker/              delivery worker
  retry-worker/        retry worker
  webhook-accept-api/  test receiver (counts deliveries, detects duplicates)
  tester/              load generator
internal/
  config/              env-var config
  event/               Event / RetryEvent types + validation
  httpapi/             HTTP handlers + JSON helpers
  kafka/               producer / consumer wrappers
  delivery/            HTTP deliverer
  breaker/             circuit breaker (with tests)
  worker/              batch consume, ordering, concurrency, retry/DLQ routing
```

## Test

```sh
make check    # build + vet + test -race
make lint     # golangci-lint (install separately)
```

## Notes

- Single broker (RF=1); fine for local, not HA.
- The test receiver dedups in memory; a real receiver would use Redis or a DB unique constraint.
- Retry delay is the retry worker sleeping until due — simple, but head-of-line blocks
  within a partition. Real systems externalize scheduling.
- Circuit-breaker state is per worker process.

## License

MIT.
