# Running with Docker

Full stack via compose:

```sh
make up       # build + start kafka, api, workers, retry-worker, receiver
make down     # stop
make reset    # stop + wipe Kafka data
```

The rest is the manual `docker build` / `docker run` path.

## Build

One binary per image, chosen by `SERVICE`:

```sh
docker build --build-arg SERVICE=api                -t webhook/api .
docker build --build-arg SERVICE=worker             -t webhook/worker .
docker build --build-arg SERVICE=retry-worker       -t webhook/retry-worker .
docker build --build-arg SERVICE=receiver -t webhook/receiver .
```

## Run

Services reach Kafka at `kafka:29092` on the compose network. Start Kafka + topics first:

```sh
docker compose up -d kafka kafka-init
```

Then (network is usually `go-proj_default`, check `docker network ls`):

```sh
docker run -d --name api --network go-proj_default \
  -e KAFKA_BROKERS=kafka:29092 -p 8000:8000 webhook/api

docker run -d --name receiver --network go-proj_default \
  -e RECEIVER_ADDRS=:8080,:8081,:8082 \
  -p 8080:8080 -p 8081:8081 -p 8082:8082 webhook/receiver

docker run -d --name worker-1 --network go-proj_default -e KAFKA_BROKERS=kafka:29092 webhook/worker
docker run -d --name worker-2 --network go-proj_default -e KAFKA_BROKERS=kafka:29092 webhook/worker
docker run -d --name worker-3 --network go-proj_default -e KAFKA_BROKERS=kafka:29092 webhook/worker
docker run -d --name retry-worker --network go-proj_default -e KAFKA_BROKERS=kafka:29092 webhook/retry-worker
```

Send an event (endpoint host is the receiver's container name; delivery happens inside the network):

```sh
curl -sXPOST localhost:8000/events \
  -d '{"endpointURL":"http://receiver:8080/webhook/demo","payload":{"hi":1}}'
curl -s localhost:8080/stats
```

The receiver serves three ports, so `receiver:8080`, `receiver:8081` and `receiver:8082` are
three separate destination hosts as far as the delivery worker is concerned. Break one of them
without touching the others:

```sh
curl -sXPOST localhost:8081/control -d '{"mode":"status","status":500}'
curl -sXPOST localhost:8081/control -d '{"mode":"ok"}'
```

Clean up:

```sh
docker rm -f api receiver worker-1 worker-2 worker-3 retry-worker
docker compose down
```
