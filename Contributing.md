# Contributing to VectorWave

Thanks for your interest in VectorWave. This guide walks through the local
development setup, how the test suite is organised, and the conventions
used for commits and pull requests.

## Quick start

```bash
# 1. Clone and install in editable mode with dev extras
git clone https://github.com/Cozymori/VectorWave.git
cd VectorWave
pip install -e ".[dev]" maturin

# 2. Compile the Rust extension (required for the high-throughput batch path)
maturin develop

# 3. Spin up a local Weaviate for running test_ex/ scripts end-to-end
vectorwave dev start
```

You should now be able to run any script under `test_ex/` and see VectorWave
write to the local Weaviate.

## Dev environment CLI

`vectorwave dev` manages a containerised Weaviate + console stack so you
don't need to wire docker-compose, env vars, and the Rust build yourself.

| Command | What it does |
|---|---|
| `vectorwave dev start` | Bring up Weaviate (8080), gRPC (50051), and the console (8081). Polls until the readiness endpoint returns 200 and prints the env vars to set. |
| `vectorwave dev stop` | Stop the containers. |
| `vectorwave dev reset` | Wipe data volumes and restart from scratch. |
| `vectorwave dev status` | Show running containers + the readiness HTTP status. |
| `vectorwave dev logs [service] [-f]` | Tail logs (default: the last 100 lines). |

Required env (set in your shell or a `.env`):

```
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8080
WEAVIATE_GRPC_PORT=50051
```

If you want vectorisation in `test_ex/` scripts, also set `OPENAI_API_KEY`.
The dev compose file picks it up so Weaviate's `text2vec-openai` module can
call it.

## Running tests

```bash
pytest                              # full suite (boots a temporary Weaviate)
pytest src/tests/database/          # one directory
pytest -m e2e                       # only end-to-end tests
pytest -m "not live"                # exclude live API tests (the default in CI)
```

The first run downloads:

- `semitechnologies/weaviate:1.28.4` (~250MB)
- `sentence-transformers/all-MiniLM-L6-v2` (~22MB) for the local HuggingFace
  vectorizer used by some search/cache tests

Both are cached for subsequent runs.

### Test categories

| Marker / location | Boots a real Weaviate? | Hits external APIs? | When it runs |
|---|---|---|---|
| Pure unit tests (no marker) | No | No | Always |
| `@pytest.mark.e2e` | Yes (testcontainers) | No | Always |
| `@pytest.mark.vcr` | Yes | Replayed from a YAML cassette | Always |
| `@pytest.mark.live` | Yes | **Real** OpenAI/Anthropic | Nightly CI only |

The session-scoped `weaviate_container` fixture starts one Weaviate per
pytest session and tears it down at the end. Tests that need DB isolation
use the `clean_weaviate` fixture which wipes the four VectorWave
collections before and after each test.

### Adding a test that calls an LLM

1. Mark it with `@pytest.mark.vcr` (and `@pytest.mark.e2e` if it also writes
   to Weaviate).
2. Run once with a real key to record:
   ```bash
   OPENAI_API_KEY=sk-... pytest path/to/test_file.py --record-mode=once
   ```
3. Inspect the generated cassette under
   `<test-dir>/cassettes/<test_module>/<test_name>.yaml`. Confirm the
   `authorization` header is `REDACTED` (the global `vcr_config` in
   `src/tests/conftest.py` strips it automatically) and that no other
   secrets slipped through (org IDs, project IDs, etc.).
4. Commit the test **and** the cassette.

Reviewers replay your cassette with no key required. The nightly `live`
workflow re-runs the same test against the real API to catch upstream
response-shape drift.

## Commits and pull requests

### Commit messages

Follow Conventional Commits with a sign-off:

```
type(scope): subject

- bullet describing what changed and why

Signed-off-by: Your Name <you@example.com>
```

Common types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`,
`ci`, `build`. Keep the subject under ~50 characters in imperative mood
("Add" not "Added"); put the "why" in the body. Examples:

```
feat(replay): support mock injection in VectorWaveReplayer
fix(batch): drain queue on shutdown to avoid losing pending writes
test(decorator): cover async @vectorize through the e2e fixture
```

### Pull requests

PRs go from a fork's branch to `Cozymori/VectorWave:main` and use this
structure (English):

```markdown
## Summary

1–3 sentences explaining the change.

## Changes

Itemised list of what was modified, with code snippets where helpful.

## Bug Fix

If applicable: root cause and how the fix addresses it.

## Test Results

How you verified the change (`pytest` output, manual verification, etc.).
```

A few things to double-check before opening a PR:

- Tests pass locally: `pytest -m "not live"`
- No secrets in cassettes: `git diff --check` and grep for `sk-`,
  `Bearer`, or other tokens before committing
- Lint is clean: `flake8 src/ --select=E9,F63,F7,F82`
- The Rust extension still builds: `maturin develop`

### What not to commit

These are gitignored or excluded by convention:

- `.env` files (secrets)
- `*.so` artefacts from `maturin develop`
- `*_cache.json`, `weaviate-data/` (local state)
- `.idea/`, `.DS_Store`, editor scratch files

## Filing issues

For bugs, please include:

- VectorWave version (`pip show vectorwave`)
- Python version + OS
- Whether the Rust core is loaded (look for the
  `[VectorWave] Rust Core Activated!` log line)
- A minimal reproduction or the relevant `vectorwave dev logs` output
