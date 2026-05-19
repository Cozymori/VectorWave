# Architectural Decision Records

This directory captures the load-bearing design decisions in VectorWave.
Each ADR is a short, dated record of a single choice: the problem we
faced, the option we picked, what we gave up, and what we considered
instead.

The point isn't documentation completeness — it's giving the next person
(future-us or a contributor) enough context to *not relitigate* a settled
question, and enough context to *know when to relitigate* if the inputs
change.

## Format

We use a lightweight [MADR](https://adr.github.io/madr/)-style template:

```
# ADR-NNNN: Title

- Status: Proposed | Accepted | Superseded by ADR-XXXX
- Date: YYYY-MM-DD

## Context
Why we needed to decide anything. What was painful or unclear before.

## Decision
What we chose. Be specific — name modules, signatures, knobs.

## Consequences
What this buys us and what it costs us. Include both.

## Alternatives Considered
Other paths we weighed, and why we didn't take them.
```

## Index

| ID | Title | Status |
|---|---|---|
| [0001](0001-vectorstore-abstraction.md) | VectorStore abstraction and Lite/Pro split | Accepted |
| [0002](0002-pytest-plugin-design.md) | Pytest plugin as primary surface for semantic regression testing | Accepted |
| [0003](0003-opentelemetry-mirror.md) | Mirror spans to OpenTelemetry instead of replacing VW's pipeline | Accepted |

## Conventions

- Numbers are assigned in order, never reused, never renumbered.
- Once Accepted, an ADR is immutable except to update its Status. If a
  later decision overrides this one, mark the old ADR `Superseded by
  ADR-XXXX` and write the new ADR — don't edit history.
- ADRs are about *decisions*, not implementations. Don't paste large
  code blocks. Cross-reference module paths instead (`src/vectorwave/...`).
