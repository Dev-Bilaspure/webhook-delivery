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
docker build --build-arg SERVICE=webhook-accept-api -t webhook/receiver .
```

## Run

Services reach Kafka at `kafka:29092` on the compose network. Start Kafka + topics first:

```sh
docker compose up -d kafka kafka-init
```

Then (network is usually `go-proj_default` — check `docker network ls`):

```sh
docker run -d --name api --network go-proj_default \
  -e KAFKA_BROKERS=kafka:29092 -p 8000:8000 webhook/api

docker run -d --name webhook-accept-api --network go-proj_default \
  -e RECEIVER_ADDR=:8080 -p 8080:8080 webhook/receiver

docker run -d --name worker-1 --network go-proj_default -e KAFKA_BROKERS=kafka:29092 webhook/worker
docker run -d --name worker-2 --network go-proj_default -e KAFKA_BROKERS=kafka:29092 webhook/worker
docker run -d --name worker-3 --network go-proj_default -e KAFKA_BROKERS=kafka:29092 webhook/worker
docker run -d --name retry-worker --network go-proj_default -e KAFKA_BROKERS=kafka:29092 webhook/retry-worker
```

Send an event (endpoint host is the receiver's container name — delivery happens inside the network):

```sh
curl -sXPOST localhost:8000/events \
  -d '{"endpointURL":"http://webhook-accept-api:8080/webhook/demo","payload":{"hi":1}}'
curl -s localhost:8080/store
```

Clean up:

```sh
docker rm -f api webhook-accept-api worker-1 worker-2 worker-3 retry-worker
docker compose down
```
