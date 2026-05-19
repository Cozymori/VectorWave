# ADR-0003: Mirror spans to OpenTelemetry instead of replacing VW's pipeline

- Status: Accepted
- Date: 2026-05-11

## Context

VectorWave's tracer captures per-call spans: timing, status, error
info, input vectors, return value, and the function's identity. These
spans are written to the `VectorWaveExecutions` collection where they
power semantic search over execution history, drift detection,
self-healing, and the golden-data pipeline.

A growing fraction of users already have an OpenTelemetry-based
observability stack — Datadog, Honeycomb, Jaeger, Grafana Tempo. Those
users want VW's per-function timings inside the same dashboards they
use for the rest of their service. The asks have been consistent:

- "I can see `@vectorize`d calls in VW's UI, but not in our Datadog
  service map."
- "Our SRE wants p95 latency by endpoint, and VectorWave's own search
  isn't where they look."

The choice was between treating VW's tracer as the primary system or as
one of several consumers of a shared trace stream.

## Decision

Add an optional OpenTelemetry exporter that **mirrors** VW spans into
the OTel pipeline. VW's own storage path is unchanged. Both write
sinks fire from the same `_perform_background_logging` step.

Specifically:

- A new module `src/vectorwave/monitoring/otel.py` constructs an OTel
  `Tracer` once per process (singleton) using the standard
  `OTEL_EXPORTER_OTLP_*` env vars.
- The tracer in `src/vectorwave/monitoring/tracer.py` calls into it
  inside `_perform_background_logging`, after VW's own write. Errors
  in the OTel path are swallowed and logged — they must not break the
  VW path.
- Activation is opt-in via `OTEL_SERVICE_NAME` (or
  `VECTORWAVE_OTEL=1`). If neither is set, the OTel path is a no-op
  and the OTel libs aren't even imported.
- The OTel deps are an optional extra: `pip install vectorwave[otel]`.

## Consequences

**Wins:**

- Existing OTel users get VW spans for free. Service maps, span search,
  alert rules — all the OTel-stack tooling works without changes on
  VW's side.
- VW's own pipeline keeps full semantic context (input vectors,
  return-value embedding, drift score). Nothing is lost in the OTel
  projection.
- Users without an OTel stack pay zero — no imports, no exporter
  threads, no extra latency. Verified by the runtime indicator
  showing `otel=off` for unconfigured processes.

**Costs:**

- Double-write on the hot path. Both VW's batch insert and the OTel
  export run in the background logger. Measured cost is well below the
  wrapper overhead (see ADR `benchmarks/v1.0-baseline`), but the cost
  isn't zero.
- The OTel projection is lossy by necessity — VW spans carry vectors
  and Python objects that don't fit OTel's attribute model. We export
  scalar attributes (status, latency, function name, hashed input
  signature) and skip the rest. Documented in the OTel section of the
  README.
- One more configuration axis to test (`otel=on/off`). CI covers the
  off-path via the main test suite and the on-path via the nightly
  e2e job that wires a local collector.

## Alternatives Considered

1. **Replace VW's pipeline with OTel-only.** Rejected — VW's storage
   isn't just "spans"; it's the substrate for semantic search, golden
   data, drift detection, and self-healing. OTel's attribute model
   can't express the vector fields without serializing them as strings,
   at which point the search-time savings disappear.
2. **OTel as primary, VW DB as optional.** Considered. Same problem
   as #1 inverted — features that depend on the VW collection would
   need a re-fetch path. Not worth the architectural inversion for the
   share of users who want OTel.
3. **No OTel integration; tell users to scrape their own.** Rejected —
   the integration is small, the user demand is real, and "we won't"
   answers the wrong question. The right question was: *primary or
   mirror?*

## References

- Related modules: `src/vectorwave/monitoring/otel.py`, `src/vectorwave/monitoring/tracer.py`.
- Related commit: `8313dc0 feat(monitoring): mirror VectorWave spans to OpenTelemetry (#29)`.
