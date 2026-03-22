---
name: test
description: Run the kafka-timebridge unit/short tests. Use when the user asks to run tests, unit tests, or wants to check that nothing is broken after a change.
allowed-tools: Bash
---

Run the kafka-timebridge short test suite (no external dependencies required):

```sh
go test -short ./...
```

`-short` skips all integration tests (Couchbase, MongoDB) and e2e tests that require a live broker.

After making code changes, also verify the build:

```sh
CGO_ENABLED=1 go build -o /dev/null ./cmd
```

## Notes
- Integration tests (Couchbase/MongoDB backends) require a live instance and are skipped with `-short`.
- E2e tests live in `./e2e/` and are skipped when `E2E_*` env vars are unset.
- To run e2e tests, use the `/e2e` skill instead.
