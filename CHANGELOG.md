# Changelog

All notable changes to VectorWave are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-05-20

The 1.0 release shifts VectorWave from a Weaviate-coupled framework into
a backend-neutral observability + regression-testing system, and folds
the standalone VectorCheck project into VectorWave as a first-class
pytest plugin.

Highlights:

- **Pytest plugin** for semantic regression testing — write a one-line
  marker, get golden-data replay + similarity assertion inside your
  normal `pytest` run.
- **Lite mode** (LanceDB) — drop Docker for local development; install
  `vectorwave[lite]` and you're running.
- **OpenTelemetry mirror** — VW spans show up in your existing OTel
  stack alongside the rest of your service traces.
- **Calibration CLI** — `vectorwave check calibrate <target>`
  replaces the magic-number threshold with a percentile-backed
  recommendation.

Benchmark anchor: `@vectorize` adds ~11.4 µs per call (median) on top
of a bare Python function on Darwin / CPython 3.12 / Apple Silicon.
Full breakdown lives in `src/tests/benchmarks/`. For any function
doing >1 ms of real work, the wrapper tax is in the noise.

### Added

- **Pytest plugin** (`vectorwave.check`). `@pytest.mark.vectorwave(target=...,
  strategy=..., threshold=..., limit=..., mocks=...)` drives a
  regression check from a marker; `vw_replay` fixture is the imperative
  variant. Config is layered from marker kwargs through
  `[tool.vectorwave.check."<target>"]` and `[tool.vectorwave.check]`
  tables in `pyproject.toml`. See ADR-0002.
- **`vectorwave check calibrate <target>` CLI**. Reports p5/p10/p25/p50/p75/p95
  of pairwise cosine similarity and recommends a threshold. Two modes:
  default `diversity` (no function calls; reads existing goldens) and
  opt-in `--rerun` (samples inputs and re-executes the function for the
  honest noise-floor measurement). Recommends `strategy="exact"` for
  deterministic targets and `strategy="llm"` for highly variable ones.
- **Lite mode** — embedded LanceDB backend via the new `VectorStore`
  protocol. Pro mode (Weaviate) remains the default; opt into Lite with
  `pip install vectorwave[lite]` and `VECTORWAVE_MODE=lite`. New
  backends are now a single-file addition. See ADR-0001.
- **OpenTelemetry mirror** — VW spans are projected into OTel when
  `OTEL_SERVICE_NAME` (or `VECTORWAVE_OTEL=1`) is set. Opt-in extras:
  `pip install vectorwave[otel]`. VW's own pipeline is unchanged — OTel
  is a mirror, not a replacement. See ADR-0003.
- **Runtime indicator** — when VectorWave activates in a process, the
  process registers itself with mode, OTel state, Rust-core status, and
  instrumented modules. `vectorwave info` lists every live VW process.
- **`vectorwave dev shell`** — drop into a subshell with `WEAVIATE_*`
  env vars exported, so demos and `test_ex/` scripts work without
  manual env wiring.
- **`vectorwave dev seed`** — insert a small demo dataset of functions
  + execution logs into a local Weaviate, no OpenAI key required.
- **Pytest-benchmark fixture** for `@vectorize` overhead measurement
  (`src/tests/benchmarks/`). Baseline saved as `v1.0-baseline`.
- **Pre-commit secret-leak hook** (`scripts/check_secrets.py`) scans
  staged content for common secret patterns before allowing the commit.
- **ADR directory** (`docs/adr/`) capturing the 1.0 load-bearing
  decisions: VectorStore abstraction, pytest plugin design, OTel mirror.

### Changed

- Vectorizer config migrated from `vectorizer_config` to `vector_config`
  (Dep024 deprecation cleanup).
- `Contributing.md` rewritten as a full contribution guide covering
  setup, the e2e fixture model, and the PR workflow.

### Fixed

- **Concurrency**: `.vectorwave_functions_cache.json` is now lock-guarded,
  and the batch manager shutdown is idempotent so duplicate calls during
  interpreter teardown no longer raise.
- **Safety**: stricter input validation, async-safety hardening in the
  tracer, parameterized filters to prevent query injection in
  Weaviate-backed lookups, and a source-leak warning when sensitive keys
  match captured arguments.
- **Cache**: `None`-returning functions can now hit the semantic cache
  via a sentinel value instead of being treated as misses.
- **Tracer**: three correctness issues in span logging — race in the
  background logger, attribute capture for kwargs-only signatures, and
  duplicate-span emission under exceptions.
- **Lite store**: `LanceVectorStore` no longer requires `pandas` as a
  hard dependency.
- **CI**: nightly live-API job exits cleanly when no `@pytest.mark.live`
  tests collect (instead of returning a "no tests selected" error).
- **Pro tests**: `clean_weaviate` fixture forces Pro mode and drops the
  cached `VectorStore` singleton so cross-test isolation holds.

### Tests / Build / CI

- Real e2e test conversion: database, healer, auto-metadata generator,
  replayer, tracer paths now run against a real backend instead of
  mocks. VCR cassettes for OpenAI-dependent tests keep them
  deterministic without burning credits.
- Testcontainers-backed Weaviate fixture replaces hand-rolled docker
  invocations.
- PR workflow updated for the e2e fixture model; nightly live-API job
  added; redundant `maturin develop` step dropped in favor of pip's
  maturin backend.
- New optional dependency groups: `lite` (LanceDB), `otel`
  (OpenTelemetry SDK + OTLP exporter), `dev` (full test toolchain
  including testcontainers and vcrpy).

### Removed

- The standalone **VectorCheck** repository (`cozymori/vectorcheck`) is
  superseded by the bundled `vectorwave.check` plugin and will be
  archived. No code-level removal in this release — the migration is
  one-directional (out of VectorCheck, into VectorWave).

## [0.3.0] - 2026-02-19

Last pre-1.0 release. See the git history for details
(`git log v0.2.9..v0.3.0`).
