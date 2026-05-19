# ADR-0002: Pytest plugin as the primary surface for semantic regression testing

- Status: Accepted
- Date: 2026-05-18

## Context

VectorWave records every `@vectorize`d call: inputs, outputs, status,
vectors. That dataset is the basis for regression testing — re-run a
function against its golden inputs and verify the output is still
"close enough" by exact match, embedding similarity, or LLM judgment.
The engine for this already lived in `SemanticReplayer`
(`src/vectorwave/utils/replayer_semantic.py`).

What did not exist was a way for users to *run* that check inside their
normal development loop. The standalone VectorCheck repo shipped a `vw
test` CLI, but that put adoption behind a separate install, a separate
config file (`vwtest.ini`), and a separate test runner that nobody's CI
already calls. Friction was high enough that the VectorCheck repo had
zero outside users.

For 1.0 we needed a way to fold this capability into VectorWave itself
with a single decision: how should users *invoke* a regression check?

## Decision

Ship a pytest plugin as the primary surface, under
`src/vectorwave/check/`. Registered via the `pytest11` entry point.

Two access patterns, both supported:

- **Declarative marker** (lead surface):

  ```python
  @pytest.mark.vectorwave(
      target="myapp.summarize",
      strategy="similarity",
      threshold=0.85,
      limit=10,
  )
  def test_summarize_regression():
      pass
  ```

  The marker fully drives the check via `pytest_pyfunc_call(tryfirst=True)`.
  The test body stays empty — the marker *is* the test.

- **Imperative fixture** (for inspection / debugging):

  ```python
  def test_summarize_regression(vw_replay):
      result = vw_replay("myapp.summarize", strategy="similarity", threshold=0.85)
      assert result.passed_all, result.report()
  ```

  Same engine, but the caller gets the `ReplayResult` object and can
  inspect failures programmatically.

Configuration is layered, highest priority wins:

1. Marker / fixture call kwargs.
2. `[tool.vectorwave.check."<target>"]` in `pyproject.toml`.
3. `[tool.vectorwave.check]` global defaults.
4. Built-in defaults (`strategy="auto"`, `threshold=0.85`, `limit=10`).

Strategy values: `exact`, `similarity`, `llm`, `auto`. For 1.0, `auto`
maps to `similarity`. Hybrid auto-escalation (similarity → LLM judge on
borderline) is deferred to a later release.

Replayer imports are deferred until a marker actually fires, so plugin
load does not touch Weaviate. This is enforced by lazy imports inside
`_run_replay`, not by convention.

## Consequences

**Wins:**

- Adoption cost is one line in `conftest.py` (or zero — entry point
  auto-loads). No new CLI, no new config file, no new test runner. The
  user keeps `pytest` and adds a marker.
- Failure output integrates with pytest's existing reporters: `-v`,
  `-x`, JUnit XML, pytest-html, IDE plugins. We didn't have to build
  any of that.
- `pyproject.toml` is the only config surface. One file, standard
  format, machine-editable. `vw calibrate --save` outputs paste-ready
  TOML for this exact reason.
- The standalone `vw test` CLI from the VectorCheck repo becomes
  redundant. VectorCheck can be archived after 1.0.

**Costs:**

- The marker hides the test body. A reader who doesn't know the plugin
  may not understand why `def test_x(): pass` is a real test. We
  mitigate with an explicit marker docstring registered in
  `pytest_configure` and a one-line note in the project README, but the
  surprise factor is real.
- The lazy-import discipline is enforced by structure, not by lint. A
  future contributor adding a module-level `SemanticReplayer()` to
  `plugin.py` would re-introduce the Weaviate-at-import problem
  silently. Captured in the module docstring.
- Calibration (`vectorwave check calibrate`) lives in the same submodule
  but is its own CLI, not a pytest mode. We accepted the split because
  calibration is a one-shot exploration step, not a per-test check.

## Alternatives Considered

1. **Keep the standalone CLI (`vw test`) and extend it.** Rejected —
   no team adds a second test runner to their CI. The friction that
   killed VectorCheck's adoption would survive intact.
2. **Fixture only, no marker.** Rejected — for the common case ("run
   regression for function X with default threshold"), the fixture
   forces three lines of boilerplate (`result = vw_replay(...)`,
   assert, report). The marker is one line.
3. **Marker only, no fixture.** Rejected — power users with custom
   golden-curation logic, batched replay across many targets, or
   non-standard assertion shapes need a programmatic handle. The
   fixture is cheap to ship and answers those use cases without forcing
   them to bypass the plugin entirely.
4. **Custom `vwtest.ini` for config, like VectorCheck did.**
   Rejected — `pyproject.toml` already exists in every Python project
   that ships, has tooling support (`tomllib`, IDE highlighting), and
   keeps VW config next to the rest of the build config.

## References

- Related memory: project-vectorwave-1-0-scope.
- Related modules: `src/vectorwave/check/plugin.py`, `src/vectorwave/check/config.py`, `src/vectorwave/check/calibrate.py`.
- Related commits: `b4a4cae feat(check): add pytest plugin for semantic regression testing`, `a02b119 feat(check): add 'vectorwave check calibrate' for threshold recommendation` (merged as upstream `9e72ea7`).
