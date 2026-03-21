# Guidance for coding agents

This file summarizes how the **kafka-timebridge** repo is structured and how changes are validated. For product behavior, configuration tables, and operations, use **[README.md](README.md)**. For commit format and pull-request checks, use **[CONTRIBUTING.md](CONTRIBUTING.md)**.

## What this project is

A Go daemon that consumes messages from a Kafka “timebridge” topic (with `X-Timebridge-When` / `X-Timebridge-Where` headers), stores them until the delivery time, and produces them to the destination topic. Optional error topic on backend failures. Backends: **memory** (default), **Couchbase**, **MongoDB**.

## Repository layout

| Path | Role |
|------|------|
| [`cmd/main.go`](cmd/main.go) | CLI entrypoint (Cobra), wiring, flags |
| [`timebridge/`](timebridge/) | Config, scheduler, acceptor, shared [`Backend`](timebridge/backend.go) interface |
| [`timebridge/memory/`](timebridge/memory/) | In-memory backend |
| [`timebridge/couchbase/`](timebridge/couchbase/) | Couchbase backend |
| [`timebridge/mongodb/`](timebridge/mongodb/) | MongoDB backend |

## Stack and constraints

- **Go** `1.26` (see [`go.mod`](go.mod)). CI uses the same version.
- **Kafka client**: `confluent-kafka-go` — **CGO is required** for builds that link librdkafka (CI uses `CGO_ENABLED=1`).
- **Node/npm** is for tooling only (`commitlint`, `semantic-release`). Use `npm ci` when validating commits or editing JS config.

## Commands to run after code changes

These mirror the **PR Check** workflow ([`.github/workflows/pr-check.yml`](.github/workflows/pr-check.yml)):

```sh
golangci-lint run
go test -short ./...
CGO_ENABLED=1 go build -o /dev/null ./cmd
```

Commit messages: see **Local validation** in [CONTRIBUTING.md](CONTRIBUTING.md) (`npm ci` then `npx commitlint`).

## Commits and pull requests

- **Every commit** on a PR must satisfy [Conventional Commits](https://www.conventionalcommits.org/) per [CONTRIBUTING.md](CONTRIBUTING.md); **semantic-release** uses them for versioning and changelog.
- PRs to `main` must pass: **lint** (golangci-lint), **commitlint**, **test** (`go test -short ./...`), **build** (`CGO_ENABLED=1 go build … ./cmd`).

## Conventions when editing

- Prefer small, task-focused diffs; follow naming and patterns in surrounding code.
- New configuration usually touches [`timebridge/config.go`](timebridge/config.go) and flag registration in [`cmd/main.go`](cmd/main.go) (env vars + CLI; flags override env).
- A new storage backend should implement [`timebridge.Backend`](timebridge/backend.go) in a package under `timebridge/<backend>/`, with tests consistent with existing backends.
- Linter config: [`.golangci.yml`](.golangci.yml) (note `errcheck` is intentionally disabled pending cleanup).
