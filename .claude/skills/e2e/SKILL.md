---
name: e2e
description: Run the kafka-timebridge end-to-end tests. Use when the user asks to run e2e tests, integration tests, or wants to validate the full message lifecycle against a real Kafka broker.
argument-hint: [--down-only] [--backend memory|mongodb|couchbase]
allowed-tools: Bash
---

Run the kafka-timebridge e2e tests following the procedure in CONTRIBUTING.md.

Before running, detect which compose tool is available:
```sh
docker compose version 2>/dev/null && COMPOSE="docker compose" || COMPOSE="podman compose"
```
Use `$COMPOSE` in place of `docker compose` for all commands below.

## Backend Selection

Parse `$ARGUMENTS` for `--backend <value>`. Default is `memory` if not specified.
The backend affects which services to start (Step 2) and which env vars to pass (Step 4).

## Procedure

```sh
export E2E_BROKER=localhost:9092
export E2E_INPUT_TOPIC=timebridge
export E2E_DEST_TOPIC=e2e-destination
```

**Step 0 — Check clock sync (podman only):**

Clock skew between the podman VM and host causes tests to fail silently (0 messages received) because Kafka rejects messages with future timestamps. Check and fix before starting the stack:

```sh
HOST_UTC=$(date -u '+%Y-%m-%d %H:%M:%S')
VM_UTC=$(podman machine ssh "date -u '+%Y-%m-%d %H:%M:%S'" 2>/dev/null)
echo "Host UTC : $HOST_UTC"
echo "VM UTC   : $VM_UTC"
```

If the times differ by more than 5 seconds, sync the VM clock:
```sh
podman machine ssh "sudo date -u -s \"$(date -u '+%Y-%m-%d %H:%M:%S')\""
```

Then verify the sync worked (`date -u` on host and VM should match within 1–2 seconds) before proceeding.

**Step 1 — Tear down any existing stack first:**
```sh
docker compose down -v
```

**Step 2 — Start services:**

| Backend | Command |
|---------|---------|
| `memory` (default) | `docker compose up -d --wait broker` |
| `mongodb` | `docker compose up -d --wait broker mongodb` |
| `couchbase` | `docker compose up -d --wait broker` then `docker compose up -d couchbase` |

**Step 2.5 — Initialize Couchbase (only if `--backend couchbase`):**

Couchbase requires cluster init and bucket/scope/collection creation via REST API:

```sh
# Wait for REST API to be reachable (up to 60s)
for i in $(seq 1 60); do
  curl -sf http://localhost:8091/ui/index.html > /dev/null && break
  sleep 1
done

# Initialize single-node cluster
curl -sf -X POST http://localhost:8091/clusterInit \
  -d 'hostname=localhost&dataPath=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&indexPath=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&services=kv%2Cn1ql%2Cindex&username=timebridge&password=123456&port=SAME'

# Create bucket
curl -sf -X POST http://localhost:8091/pools/default/buckets \
  -u timebridge:123456 \
  -d 'name=timebridge&bucketType=couchbase&ramQuota=256'

# Wait for bucket readiness
sleep 5

# Create scope
curl -sf -X POST http://localhost:8091/pools/default/buckets/timebridge/scopes \
  -u timebridge:123456 \
  -d 'name=timebridge'

# Create collection
curl -sf -X POST http://localhost:8091/pools/default/buckets/timebridge/scopes/timebridge/collections \
  -u timebridge:123456 \
  -d 'name=messages'

# Wait for collection to be queryable
sleep 3
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

| Backend | Command |
|---------|---------|
| `memory` (default) | `SCHEDULER_POLL_INTERVAL_SECONDS=1 LOG_LEVEL=debug KAFKA_TOPIC=timebridge docker compose up -d --build timebridge` |
| `mongodb` | `BACKEND=mongodb SCHEDULER_POLL_INTERVAL_SECONDS=1 LOG_LEVEL=debug KAFKA_TOPIC=timebridge docker compose up -d --build timebridge` |
| `couchbase` | `BACKEND=couchbase SCHEDULER_POLL_INTERVAL_SECONDS=1 LOG_LEVEL=debug KAFKA_TOPIC=timebridge docker compose up -d --build timebridge` |

Note: The compose file already has all `MONGODB_*` and `COUCHBASE_*` connection env vars hardcoded for the timebridge service. Only `BACKEND` needs to change.

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

**Tests timeout with 0 messages received (clock skew)**: The container VM's clock has drifted from the host — common after macOS sleep/wake with podman. Timebridge logs will show messages stored with `remains=Xm` far in the future. Run Step 0 to detect and fix, then restart from Step 1.

**Messages silently dropped with "Broker: Invalid timestamp"**: Same root cause as above — clock skew. Run Step 0 and restart the stack.

**Tests timeout with 0 messages received (poll interval)**: Check that `SCHEDULER_POLL_INTERVAL_SECONDS=1` was passed when starting timebridge (Step 4). Verify with `docker compose logs timebridge | grep "next_retry"` — it should show `next_retry=1s`.

**Couchbase: "index not found" or "bucket not found"**: The Couchbase init in Step 2.5 may not have completed. Check `curl -s http://localhost:8091/pools/default/buckets -u timebridge:123456` and verify the bucket exists. Re-run Step 2.5 if needed.
