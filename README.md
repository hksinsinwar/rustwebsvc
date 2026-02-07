# Rust Web Service (Axum + MySQL + Redis + Kafka)

This service demonstrates end-to-end flows using Axum with MySQL, Redis, and Kafka.
It supports JSON and Protocol Buffers payloads and responses.

## Configuration

Set the following environment variables (defaults are shown):

- `REDIS_URL` (default: `redis://127.0.0.1/`)
- `MYSQL_URL` (default: `mysql://root:password@127.0.0.1:3306/app`)
- `KAFKA_BROKERS` (default: `127.0.0.1:9092`)
- `KAFKA_TOPIC` (default: `events`)

Run the service:

```bash
cargo run
```

## JSON examples

### Redis cache

```bash
curl -X POST http://localhost:3000/cache/demo \
  -H 'Content-Type: application/json' \
  -d '{"value":"hello"}'

curl -X GET http://localhost:3000/cache/demo \
  -H 'Accept: application/json'
```

### MySQL users

```bash
curl -X POST http://localhost:3000/users \
  -H 'Content-Type: application/json' \
  -d '{"name":"Ada","email":"ada@example.com"}'

curl -X GET http://localhost:3000/users/1 \
  -H 'Accept: application/json'
```

### Kafka event

```bash
curl -X POST http://localhost:3000/events \
  -H 'Content-Type: application/json' \
  -d '{"key":"demo","payload":"event"}'
```

## Protobuf examples

The protobuf schema lives at `proto/messages.proto`. Use it to generate client bindings.

Example using `buf` + `protoc` to encode payloads is left to your client setup. The endpoints
expect `Content-Type: application/x-protobuf` and will return protobuf when
`Accept: application/x-protobuf` is provided.

## Tests

```bash
cargo test
```

The tests use a mock backend to validate JSON/protobuf handling without external services.
