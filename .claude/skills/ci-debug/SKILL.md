---
name: ci-debug
description: Diagnose GitHub Actions CI failures for kafka-timebridge. Use when the user asks to check CI, debug a failed run, or wants to know why a workflow failed.
argument-hint: [run-id]
allowed-tools: Bash
---

Diagnose a GitHub Actions run for `martavoi/kafka-timebridge`.

## Run ID Resolution

Parse `$ARGUMENTS` for a numeric run ID. If none is provided, use the latest completed run on `main`:

```sh
RUN_ID="${ARGUMENTS:-$(gh run list \
  --repo martavoi/kafka-timebridge \
  --branch main --limit 10 \
  --json databaseId,status \
  --jq '[.[] | select(.status != "in_progress")] | first | .databaseId')}"
```

## Procedure

**Step 1 — Job summary:**
```sh
gh run view "$RUN_ID" --repo martavoi/kafka-timebridge
```

**Step 2 — If any job failed, show failure logs (first 200 lines):**
```sh
gh run view "$RUN_ID" --repo martavoi/kafka-timebridge --log-failed 2>&1 | head -200
```

**Step 3 — If an e2e job failed, extract timebridge container logs from the "Show logs on failure" step:**
```sh
gh run view "$RUN_ID" --repo martavoi/kafka-timebridge --log 2>&1 \
  | grep -A 2000 "Show logs on failure" \
  | grep "timebridge-1" \
  | head -100
```

## Output

Summarise:
- Which jobs passed and which failed
- The error message or root cause
- A suggested fix if one is apparent

## Notes

- `gh` is available and `Bash(gh run:*)` is permitted in `settings.local.json`.
- The `e2e (couchbase)` job failure logs will include N1QL errors, scheduler backoff messages, and acceptor ingestion lines — look for `level=ERROR` lines in the timebridge container output.
- If `$ARGUMENTS` is a GitHub Actions URL (e.g. `https://github.com/.../runs/12345`), extract the numeric run ID from the URL and use it.
