# ADR-0001: VectorStore abstraction and Lite/Pro split

- Status: Accepted
- Date: 2026-05-10

## Context

VectorWave shipped with Weaviate hard-coded as the only backend. That
meant:

- Every test, demo, and contributor onboarding required Docker + a
  Weaviate container. The README's "quick start" took 5+ minutes on a
  cold machine.
- Storage logic was spread across `database/db.py`, `database/dataset.py`,
  the tracer, the replayer, and the auto-injector. Every new feature
  reimplemented "query collection X with filter Y."
- Functions like `get_cached_client()` and the Rust `RustBatchManager`
  assumed Weaviate semantics (gRPC client, server-side vectorization)
  and could not be swapped without rewriting callers.
- We wanted a low-friction local-only mode for quick iteration, but
  could not introduce one without touching every storage call site.

A backend split was necessary before any further feature work landed.

## Decision

Introduce a `VectorStore` protocol in `src/vectorwave/store/base.py` that
defines the operations the rest of VectorWave actually needs:

- `query(collection, filters, sort_by, limit) -> records`
- `fetch_by_id(collection, uuid) -> record | None`
- `update(collection, uuid, properties)`
- `insert(collection, properties)`
- collection-existence + schema-management helpers

Two backends implement the protocol:

- **Pro mode** (`store/weaviate_store.py`) — the existing Weaviate path,
  including server-side vectorization, the Rust batch manager, and the
  golden-data lifecycle. Default for anyone who already has a Weaviate
  instance configured.
- **Lite mode** (`store/lance_store.py`) — embedded LanceDB. Zero
  Docker, zero ports, single on-disk directory. Optional dependency
  (`pip install vectorwave[lite]`).

A single entry point `get_vector_store()` (in `src/vectorwave/store/__init__.py`)
returns the singleton for the configured mode. All other modules call
this — no direct Weaviate or Lance imports anywhere else.

Mode selection is environment-driven (`VECTORWAVE_MODE=lite|pro`,
defaults to Pro for backward compatibility).

## Consequences

**Wins:**

- New backends are a single file. The PR that added LanceDB touched one
  new module + the factory; no call-site changes elsewhere.
- Local tests run without Docker. `pytest src/tests/` against the Lite
  backend completes in a fraction of the time it took to spin up
  Weaviate.
- Storage concerns are now centralized. Future migrations (schema
  changes, batching strategy changes) live in one place.
- Pro-mode behavior is unchanged for existing users — `VECTORWAVE_MODE`
  defaults preserve the prior wiring.

**Costs:**

- A small set of Weaviate features (server-side `text2vec-openai`,
  hybrid search, distributed batching) are unavailable in Lite mode.
  This is documented in the Lite mode section of the README and
  surfaces as runtime warnings, not silent fallback.
- Two backends means two test matrices in CI (Pro via testcontainers
  Weaviate, Lite via temp directory). The cost is real but bounded.
- The protocol records (`StoreRecord`, etc.) leak some Weaviate-shaped
  ergonomics (`.uuid`, `.properties`). We accept this for 1.0 and
  revisit if/when a third backend arrives.

## Alternatives Considered

1. **Stay on Weaviate, fix DX instead.** Improve testcontainers setup,
   add docker-compose presets. Rejected — the Docker dependency itself
   was the friction; faster Docker is still Docker.
2. **Use a generic vector-DB ORM** (e.g. LangChain's vector store
   interface). Rejected — those abstractions are CRUD-only and miss the
   Weaviate-specific features VectorWave already relies on (filters,
   custom properties, golden-data joins). Wrapping a wrapper would have
   doubled the abstraction without solving the friction.
3. **Per-call backend selection.** Let each `@vectorize` choose its
   store. Rejected — every VW deployment we know of standardizes on one
   backend per environment; per-call selection adds config surface
   nobody asked for.

## References

- Related memory: project-architecture-shift-2026-05.
- Related commits: `2b0107a feat(store): add Lite mode (LanceDB) via VectorStore abstraction (#95)`, `a118193 feat(store): finish Lite mode coverage across read/write paths`.
