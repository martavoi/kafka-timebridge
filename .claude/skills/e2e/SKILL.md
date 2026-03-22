---
name: e2e
description: Run the kafka-timebridge end-to-end tests. Use when the user asks to run e2e tests, integration tests, or wants to validate the full message lifecycle against a real Kafka broker.
argument-hint: [--down-only]
allowed-tools: Bash
---

Run the kafka-timebridge e2e tests following the procedure in CONTRIBUTING.md.

Before running, detect which compose tool is available:
```sh
docker compose version 2>/dev/null && COMPOSE="docker compose" || COMPOSE="podman compose"
```
Use `$COMPOSE` in place of `docker compose` for all commands below.

## Procedure

```sh
export E2E_BROKER=localhost:9092
export E2E_INPUT_TOPIC=timebridge
export E2E_DEST_TOPIC=e2e-destination
```

**Step 1 — Tear down any existing stack first:**
```sh
docker compose down -v
```

**Step 2 — Start broker and wait for healthy:**
```sh
docker compose up -d --wait broker
```

**Step 3 — Create topics:**
```sh
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic timebridge --partitions 1 --replication-factor 1

docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic e2e-destination --partitions 1 --replication-factor 1
```

**Step 4 — Build and start timebridge with fast poll interval:**
```sh
SCHEDULER_POLL_INTERVAL_SECONDS=1 LOG_LEVEL=debug KAFKA_TOPIC=timebridge \
  docker compose up -d --build timebridge
```

**Step 5 — Run the tests:**
```sh
E2E_BROKER=localhost:9092 \
E2E_INPUT_TOPIC=timebridge \
E2E_DEST_TOPIC=e2e-destination \
CGO_ENABLED=1 go test -v -timeout 120s -count=1 ./e2e/
```

**Step 6 — Show logs if any test fails:**
```sh
docker compose logs timebridge
docker compose logs broker
```

**Step 7 — Tear down:**
```sh
docker compose down -v
```

## Notes
- `CGO_ENABLED=1` is required — confluent-kafka-go uses cgo.
- `SCHEDULER_POLL_INTERVAL_SECONDS=1` is critical; the default is 5s which causes timeouts.
- The e2e tests skip automatically when `E2E_*` env vars are unset (e.g. `go test -short`).
- If `$ARGUMENTS` is `--down-only`, only run Step 1 (tear down) and stop.

## Troubleshooting

**Messages silently dropped with "Broker: Invalid timestamp"**: The container VM's clock has drifted from the host (common after macOS sleep/wake with podman). Two options:
- Sync the clock in place: `podman machine ssh "sudo date -u -s \"$(date -u '+%Y-%m-%d %H:%M:%S')\""`
- Or restart the VM entirely: `podman machine stop && podman machine start` (also fixes the clock, slightly slower)

After either fix, restart the stack from Step 1 so Kafka starts with the corrected time.

**Tests timeout with 0 messages received**: Check that `SCHEDULER_POLL_INTERVAL_SECONDS=1` was passed when starting timebridge (Step 4). Verify with `docker compose logs timebridge | grep "next_retry"` — it should show `next_retry=1s`.
