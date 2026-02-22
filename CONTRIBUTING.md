# Contributing to Kafka Timebridge

## Commit Messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/). Commit messages are the sole source of truth for versioning and changelog generation — semantic-release reads them to decide whether to cut a patch, minor, or major release.

Every commit must follow this format:

```
<type>(<optional scope>): <short description>

<optional body>

<optional footer>
```

### Allowed Types

| Type | Triggers Release | Description |
|------|-----------------|-------------|
| `feat` | minor | A new feature |
| `fix` | patch | A bug fix |
| `perf` | patch | A performance improvement |
| `refactor` | patch | Code restructuring without behaviour change |
| `revert` | patch | Reverts a previous commit |
| `docs` | — | Documentation changes only |
| `style` | — | Formatting, whitespace, missing semicolons |
| `chore` | — | Maintenance tasks (deps, tooling) |
| `test` | — | Adding or fixing tests |
| `build` | — | Build system or external dependency changes |
| `ci` | — | CI configuration changes |

### Breaking Changes

Add `BREAKING CHANGE:` to the commit footer, or append `!` after the type. Either triggers a **major** release.

```
feat!: remove legacy flat config support

BREAKING CHANGE: the --config-file flag no longer accepts flat JSON format.
Use the nested format documented in README.md instead.
```

### Scopes (Optional)

Scopes provide additional context. Use the relevant subsystem:

```
feat(couchbase): add TTL support for stored messages
fix(kafka): handle rebalance during long-running delivery
```

### Examples

Good:
```
feat: add per-topic retry backoff configuration
fix: prevent duplicate delivery on consumer rebalance
docs: document KAFKA_GROUP_ID environment variable
ci: add ARM64 matrix build
```

Bad (will fail the PR check):
```
update stuff
WIP
fixed bug
```

## Pull Request Process

1. Branch from `main`
2. Make your changes with well-formed commits
3. Open a PR — the `PR Check / commitlint` status check will validate every commit automatically
4. All commits in the PR must pass before merging

## Local Validation

After running `npm install`, you can validate your last commit locally:

```sh
npx commitlint --edit
```

Or validate a specific commit message:

```sh
echo "feat: my new feature" | npx commitlint
```
