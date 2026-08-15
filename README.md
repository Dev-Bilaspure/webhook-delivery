# webhook-delivery

Accepts webhook events over HTTP and delivers them to customer endpoints. Kafka sits
between accept and deliver, so accepting is fast and durable while delivery is async,
with retries, exponential backoff, a dead-letter queue, per-endpoint ordering,
idempotency, concurrency limits, and per-host circuit breaking.

Built to learn Go and distributed-systems patterns. Single-broker local setup; see [Notes](#notes).

## Architecture

```
POST /events
     │  202 once durable (acks=all)
     ▼
  ┌─────┐  keyed by orderingKey   ┌──────────────────┐
  │ API │ ───────────────────────►│ events (3 parts) │
  └─────┘  same key → same part   └────────┬─────────┘
                                           │ batch of 50, grouped by key
                                           ▼
                                  ┌───────────────────┐
                                  │  delivery worker  │  groups concurrent,
                                  │  breaker · sems   │  serial within a group
                                  └────────┬──────────┘
                                           │ POST to endpoint
             ┌────────────────┬────────────┴───────┬──────────────────┐
             ▼                ▼                    ▼                  ▼
            2xx       5xx / timeout / 429    breaker open       4xx / expired
             │                │                    │                  │
         committed            └────────┬───────────┘                  │
                                       ▼                              ▼
                                ┌─────────────┐              ┌─────────────┐
                           ┌───►│   retries   │              │ dead-letter │
                           │    └──────┬──────┘              └─────────────┘
                           │           │                            ▲
                    still failing      ▼                            │
                           │    ┌──────────────┐        retries exhausted
                           └────┤ retry worker │        or past MAX_EVENT_AGE
                                │ waits until  ├────────────────────┘
                                │ NextRetryAt  │
                                └──────────────┘
```

- **API** validates, publishes to `events` with `acks=all`, returns 202. The 202 means durable
  in Kafka, not delivered.
- **Ordering key** is the partition key, so a key's events land on one partition in order. The
  worker groups a batch by key and walks each group serially: parallel across keys, sequential
  within one. On failure the rest of that key's group is diverted too, so nothing overtakes it.
- **Breaker and semaphores** gate every delivery. An open breaker skips the request entirely and
  costs no concurrency slot; the per-host semaphore stops one destination from consuming the
  global pool.
- **Three outcomes.** 2xx commits. Retryable failures (5xx, 429, timeouts) go to `retries` with
  exponential backoff. Permanent failures (most 4xx) and events past `MAX_EVENT_AGE` go straight
  to `dead-letter` without spending retry budget.
- **The retry loop is a loop.** The retry worker waits until an event is due, then redelivers,
  and a failure puts it back in `retries`. It leaves only by succeeding, exhausting its retries,
  or ageing out.

## Guarantees

- **At-least-once delivery.** Every event accepted with a 202 is either delivered at least
  once or durably retained where you can inspect it. Nothing is dropped silently.
- **Ordering per key, within a batch.** Events sharing an `orderingKey` hash to one partition
  and are delivered sequentially. On failure the whole remaining tail for that key is diverted
  with the failed event, so later events cannot overtake it.
- **Bounded concurrency**, globally and per destination host.
- **Failing hosts isolated** by a per-host circuit breaker, which recovers on its own.
- **Permanent rejections cost nothing.** A 4xx that cannot succeed on retry goes straight to
  the DLQ without consuming retry budget or counting against the host's breaker.
- **Bounded event lifetime.** Nothing lives past `MAX_EVENT_AGE` (default 1h), regardless of
  how many times it was deferred.

## Non-guarantees

Worth reading before the guarantees above sound stronger than they are.

- **Not exactly-once.** Receivers must honour `Idempotency-Key`. Duplicates are the price of
  never losing an event. See [duplicates](#where-duplicates-come-from).
- **No ordering across the failure → recovery transition.** `events` and `retries` are drained
  by independent consumer groups with no coordination. When an outage ends, fresh events flow
  straight through while earlier ones are still waiting out their backoff. The inversion is
  systematic rather than occasional: the backoff ladder schedules the oldest messages furthest
  out, and an open breaker guarantees a backlog exists exactly when direct delivery resumes.
- **One slow destination slows all of them.** A batch commits only after every message in it
  reaches a terminal state, and the next batch is not fetched until then, so the pipeline runs
  at the speed of its slowest destination. The per-host bulkhead correctly caps a bad host's
  share of the concurrency; it cannot do anything about the batch barrier downstream.
- **Throughput is bounded by distinct ordering keys.** A single hot key serialises.
- **No global ordering** across keys.
- **A publish failure to `retries`/`dead-letter` costs a batch of duplicate deliveries.**
- **`Retry-After` is ignored.** A 429 is retried on our own backoff ladder.
- Single broker, RF=1. Fine for local, not HA.
- Circuit-breaker state is per worker process, not shared.

## Results

Produced by `make bench` on this machine; see [Benchmarking](#benchmarking) for how, and
`bench/` for the raw JSON.

3 destination hosts, 400 ordering keys, 3000 events per run. Median of 3 reps, range in
brackets. Steady runs offer 300 events/sec; burst runs use 100 unpaced submitters.

| scenario | profile | accept/s | p50 ms | p95 ms | arrived | dlq | inversions |
| --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | steady | 300 | 114.1 (114.0-114.3) | 205.7 | 3000 | 0 | 0 |
| baseline | burst | 7265 (7216-7519) | 27.6 | 42.9 | 3000 | 0 | 0 |
| permanent | steady | 299 | 120.2 | 217.0 | 3000 | 998 | 0 |
| permanent | burst | 7662 (7239-7699) | 26.9 | 40.7 | 3000 | 998 | 0 |
| bulkhead | steady | 300 | 5441 (5326-42443) | 49024 (9890-54836) | 2109 | 0 | 0 |
| breaker | steady | 299 | 171.0 | 45195 (45194-45199) | 3006 | 0 | 0 |
| chaos | steady | 300 | 198.7 | 90121 (45207-135119) | 3886 | 0 | 8 (0-34) |

**baseline** is the honest steady-state number: 300/s sustained, every event delivered exactly
once, ordering held across 400 keys. Reproducible to 0.3ms of p50 across reps.

**permanent** matches to the integer, every run: 998 refused by the host, 998 dead-lettered,
**zero retries**. A 4xx that cannot succeed on retry does not spend retry budget.

**breaker** is the strongest result. About 1000 events (the failing host's third) were deferred
once, the host was healed at t+30s, and every one of them delivered: **zero lost, zero
dead-lettered, 6 duplicates in 3000 (0.2% redelivery)**. p95 of 45,195ms is one `BREAKER_COOLDOWN`
to the millisecond, which is the deferred events waiting out the cooldown.

**chaos** is where the documented ordering non-guarantee shows up as a number: 8 inversions
(range 0-34) across 400 keys when hosts flap. Zero on every clean run. This is measured rather
than asserted, because the recovery transition genuinely does invert order and a test that
demanded zero would be testing the wrong thing.

**bulkhead** is the limitation, not the feature, and the per-host split is the point:

| host | delivered | p50 | p95 |
| --- | --- | --- | --- |
| `:8080` healthy | 1005 | 5,144ms | 9,518ms |
| `:8082` healthy | 997 | 5,103ms | 9,529ms |
| `:8081` hanging 15s | 138 | 94,028ms | 480,270ms |

The per-host cap did its job: the hanging host was held to 5 of the 10 concurrency slots and both
healthy hosts delivered every event they were sent. But their p50 went from 114ms to 5,144ms, a
45x degradation caused entirely by a different host hanging. That is the batch barrier in
[Non-guarantees](#non-guarantees): a batch commits only when its slowest message finishes, and
nothing new is fetched until it does, so the pipeline runs at the speed of its worst destination.
Bounding concurrency per host does not bound the damage.

~900 events per run were still cycling in `retries` when the wait gave up, which is why `arrived`
is ~2100 rather than 3000. They are outstanding against a permanently hanging host, not lost:
`dlq +0` says nothing gave up.

One caveat on `baseline/burst`: the first rep of three reported 63 events outstanding because a
breaker opened by the preceding scenario was still in its cooldown when the run started. Breaker
state lives in the worker process and outlives a scenario, which the drain check cannot see.
`scripts/bench.sh` now waits out a cooldown after any fault scenario. Reps 2 and 3 were clean and
the median reflects them.

### What load testing found

Two production bugs, neither visible by reading the code.

**A one-second producer batch timeout.** Accept throughput came out exactly equal to client
concurrency: 20 workers gave 20/s, 60 gave 60/s. Perfectly linear, which is the signature of a
fixed per-request delay rather than a resource limit; real capacity limits are noisy and
plateau. `kafka-go` flushes a batch on `BatchSize` (100) or `BatchTimeout`, and that timeout
defaults to one second. An API request path publishes one message and blocks for the ack, so
the batch never filled by size and every publish waited out the full second.

| | before | after |
| --- | --- | --- |
| accept rate, concurrency 20 | 20/s | 1,350/s |
| accept rate, concurrency 60 | 60/s | 4,043/s |
| first-attempt p50 | 1,085ms | 16ms |

Durability is unchanged. `RequiredAcks: RequireAll` and synchronous writes, so 202 still means
durably in Kafka. Set `PRODUCER_BATCH_TIMEOUT=1s` to reproduce the old behaviour.

**An undersized HTTP connection pool.** `http.DefaultTransport` allows 2 idle connections per
host against a configured concurrency of 5, so every request past the second opened a fresh TCP
connection and discarded it. Sizing the pool to the concurrency cut discarded connections from
741 to 455 per 3,000 deliveries, with **no latency change**, because on loopback a handshake is
effectively free. The saving only materialises across a real network, where each avoided
handshake is a round trip. Reported here as a null result rather than a delta that does not exist.

### Where duplicates come from

Delivering and committing an offset cannot be made atomic. Commit first and a crash in between
loses the event; deliver first and a crash duplicates it. Deliver-first is chosen, so loss is
impossible and duplicates are not.

They come from two places: anything that stops an offset commit (crash, shutdown mid-batch, a
failed publish to `retries`, a consumer group rebalance), and ambiguous outcomes where the
delivery succeeded but the response never arrived. The second needs no crash at all. It is why
idempotency keys exist.

In a clean run duplicates should be zero. They are the price of failure handling, not a
constant tax.

## Benchmarking

The harness is a bigger piece of work than the service it measures. The service is a black box
here; everything else is test infrastructure.

```
                   ┌──────────────────────────────────────┐
                   │          scripts/bench.sh            │
                   │                                      │
                   │  clear faults                        │
                   │  wait for consumer lag = 0           │
                   │  reset store                         │
                   │  set this scenario's fault           │
                   │  run tester, capture JSON            │
                   └──┬────────────────┬───────────────┬──┘
                      │                │               │
   POST /control ─────┘           run  │               └───── docker exec:
   POST /reset                         │                      lag, topic depth
        │                              ▼                            │
        │              ┌───────────────────────────┐                │
        │              │        cmd/tester         │                │
        │              │  steady: ticker +         │                │
        │              │          free-key pool    │                │
        │              │  burst:  N goroutines     │                │
        │              └─────────────┬─────────────┘                │
        │                            │ POST /events                 │
        │                            ▼                              ▼
        │   ╔══════════════════════════════════════════════════════════╗
        │   ║  SYSTEM UNDER TEST                                       ║
        │   ║  api · kafka · delivery worker · retry worker            ║
        │   ╚═════════════════════════╤════════════════════════════════╝
        │                             │ POST /webhook/{key}
        ▼                             ▼
   ┌──────────────────────────────────────────────────────────┐
   │  cmd/receiver                                            │
   │                                                          │
   │     :8080          :8081          :8082                  │
   │       │              │              │      three fake    │
   │   injector       injector       injector      customers  │
   │       └──────────────┼──────────────┘                    │
   │                      ▼                                   │
   │               one shared store                           │
   │      key · seq · addr · status · arrived · latency       │
   └──────────────────────────────────────────┬───────────────┘
                                              │
              cmd/tester reads back ──────────┘
              GET /stats?prefix=  ·  GET /keys?prefix=
              once a second, until every submitted id has
              arrived or nothing new has landed for a while
                          │
                          ▼
        bench/<scenario>-<profile>-<rep>.json
                          │
                          ▼
               scripts/summarise.py
               median and range across reps
```

Three separate concerns, which is why the receiver has a control endpoint at all:

- **Control** goes down the left. `bench.sh` sets each fake customer's behaviour at runtime, and
  can change it mid-run, which is the only way to watch a circuit breaker recover.
- **Data** goes down the middle. The tester submits, the system delivers, the receiver answers.
- **Measurement** comes back from two places. The receiver knows what arrived and when; Kafka
  knows what was dead-lettered and retried. Neither alone can tell you whether an event was lost.

```sh
make bench            # every scenario, both load profiles, 3 reps, then the table
make bench-quick      # one rep, steady only
make bench-summary    # reprint the table from bench/
```

Scenarios: `baseline`, `permanent` (a host returning 400), `bulkhead` (a host hanging past the
delivery timeout), `breaker` (a host failing, then healed mid-run), `chaos` (all hosts flapping).

Two load profiles, because they answer different questions:

- **steady**: open loop, fixed rate from a ticker. Latency at a *stated* offered rate.
- **burst**: closed loop, fixed concurrency, unpaced. Capacity.

Quoting burst latency would be picking the arrival pattern that flatters the result: dumping
everything at once fills Kafka batches by size, so most events skip the fill wait that real
spread-out traffic pays. Headline latency comes from steady runs, with the rate stated.

### Making the numbers trustworthy

Three things, because a benchmark that is only correct when a timeout happens to be tuned right
is lucky rather than reliable:

- **Each run namespaces its ordering keys** (`r847392615-cust-0`), so events still draining from
  an earlier run cannot collide with a live key's sequence numbers and read as ordering
  violations.
- **The receiver reports per prefix**, so leftovers can arrive mid-run without entering this
  run's percentiles or counts.
- **The drain check waits on consumer lag**, not on arrivals pausing. A 45s breaker cooldown
  looks identical to being finished. If the system has not drained it skips the run and says so.

The first two hold even if the third is wrong. Note the Kafka topic deltas (`dlq`, `retries`) are
system-wide and cannot be prefix-scoped, so they are only meaningful when the drain check passed.

## Run (Docker)

```sh
make up       # build + start kafka, api, 3 workers, retry-worker, receiver
make ps
make logs
make down     # stop (make reset also wipes Kafka data)
```

Send one. The endpoint host is the receiver's service name, since delivery runs inside
the Docker network:

```sh
curl -sXPOST localhost:8000/events \
  -d '{"endpointURL":"http://receiver:8080/webhook/demo","payload":{"hello":"world"}}'
curl -s localhost:8080/stats
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

Env vars, all with defaults, see [`.env.example`](.env.example).

## Layout

```
cmd/
  api/                 HTTP API (accepts events)
  worker/              delivery worker
  retry-worker/        retry worker
  receiver/            test receiver (fault injection + delivery metrics)
  tester/              load generator
internal/
  config/              env-var config
  event/               Event / RetryEvent types + validation
  httpapi/             HTTP handlers + JSON helpers
  kafka/               producer / consumer wrappers
  delivery/            HTTP deliverer + status classification
  breaker/             circuit breaker (with tests)
  receiver/            fault injector, sample store, handlers (with tests)
  worker/              batch consume, ordering, concurrency, retry/DLQ routing
scripts/
  bench.sh             runs a scenario, writes JSON to bench/
  summarise.py         the results table
```

The test receiver serves three ports so the delivery worker sees three distinct destination
hosts; per-host isolation is unprovable against a single address. Faults are set at runtime:

```sh
curl -XPOST localhost:8081/control -d '{"mode":"status","status":500}'
curl -XPOST localhost:8081/control -d '{"mode":"hang","delay":"15s"}'
curl -XPOST localhost:8081/control -d '{"mode":"flap","status":500,"failures":3,"successes":10}'
curl -XPOST localhost:8081/control -d '{"mode":"ok"}'
```

Runtime control matters: a breaker only demonstrates recovery if the host can be healed while
load is still flowing.

## Test

```sh
make check    # build + vet + test -race
make lint     # golangci-lint (install separately)
```

## Notes

- Single broker (RF=1); fine for local, not HA.
- The test receiver dedups in memory; a real receiver would use Redis or a DB unique constraint.
- Retry delay is the retry worker sleeping until due, which stalls its batch's offset commit.
  Delay topics (`retries-5s`, `retries-1m`) or an external scheduler would fix it.
- The obvious fix for the batch barrier, committing per group as it finishes, is wrong. A
  Kafka offset commit is a watermark, not a set, so committing a fast group's highest offset
  marks a slower group's earlier messages as done while they are still in flight. The correct
  version is contiguous-prefix commit with decoupled fetching, which is where at-least-once bugs
  live.
- Circuit-breaker state is per worker process.

## License

MIT.
