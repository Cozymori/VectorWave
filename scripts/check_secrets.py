#!/usr/bin/env python3
"""Block credential leaks at commit time.

Runs as a pre-commit hook over the staged file list (passed on argv) and
fails if it finds anything that looks like a real LLM API key, bearer token,
or non-redacted authorization value.

The most common foot-gun is committing a freshly-recorded VCR cassette that
hasn't had its `authorization` header masked. The session-level vcr_config
in `src/tests/conftest.py` strips that header at record time, but a manually
edited cassette could still slip through — this hook is the second line of
defence.

Tested patterns (case-insensitive):
- OpenAI keys: `sk-proj-...`, `sk-svcacct-...`, `sk-...` (legacy)
- Anthropic keys: `sk-ant-...`
- Generic Bearer/auth headers carrying a long token
- AWS access keys: `AKIA[0-9A-Z]{16}`
- Common env-style assignments: `OPENAI_API_KEY=sk-...`

Allowed (intentionally short / placeholder):
- `sk-test-...` (used in test fixtures)
- `REDACTED`, `<...>`, `xxx...` placeholders
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

# Pattern → human-readable label.
_SECRET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"sk-(?:proj|svcacct|admin)-[A-Za-z0-9_\-]{20,}"), "OpenAI project/service key"),
    (re.compile(r"sk-ant-[A-Za-z0-9_\-]{20,}"), "Anthropic key"),
    # Legacy OpenAI keys: sk- followed by 30+ chars, but explicitly not "sk-test-..."
    (re.compile(r"\bsk-(?!test-|fake-|dummy-|REDACTED)[A-Za-z0-9]{30,}"), "OpenAI legacy key"),
    (re.compile(r"AKIA[0-9A-Z]{16}"), "AWS access key id"),
    (re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-]{30,}"), "Bearer token in plaintext"),
    (re.compile(r"(?i)authorization:\s*['\"]?(?!REDACTED|<|\[REDACTED)([A-Za-z]+\s+)?[A-Za-z0-9._\-]{30,}"),
     "non-redacted Authorization header"),
]

# File extensions worth scanning (binary or generated files skipped).
_SCAN_SUFFIXES = {".py", ".yaml", ".yml", ".json", ".env", ".toml", ".md", ".txt", ".cfg", ".ini", ".sh"}

# Files to never scan (already gitignored or build artefacts).
_SKIP_FILES = {".vectorwave_functions_cache.json"}


def _should_scan(path: Path) -> bool:
    if path.name in _SKIP_FILES:
        return False
    if path.suffix.lower() not in _SCAN_SUFFIXES:
        # Cassettes might land in cassettes/ without a recognised suffix; scan defensively
        return "cassettes" in path.parts
    return True


def _scan_file(path: Path) -> list[tuple[int, str, str]]:
    """Returns a list of (line_no, label, snippet) for each match in the file."""
    findings: list[tuple[int, str, str]] = []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        print(f"[check_secrets] warn: could not read {path}: {e}", file=sys.stderr)
        return findings
    for lineno, line in enumerate(text.splitlines(), start=1):
        for pattern, label in _SECRET_PATTERNS:
            m = pattern.search(line)
            if m:
                snippet = line.strip()
                if len(snippet) > 120:
                    snippet = snippet[:117] + "..."
                findings.append((lineno, label, snippet))
                break  # one finding per line is enough
    return findings


def main(argv: list[str]) -> int:
    files = [Path(p) for p in argv]
    failures: list[tuple[Path, int, str, str]] = []
    for f in files:
        if not f.is_file() or not _should_scan(f):
            continue
        for lineno, label, snippet in _scan_file(f):
            failures.append((f, lineno, label, snippet))

    if not failures:
        return 0

    print("\nBlocked: possible secret(s) in staged files:\n", file=sys.stderr)
    for path, lineno, label, snippet in failures:
        print(f"  {path}:{lineno}  [{label}]", file=sys.stderr)
        print(f"    >> {snippet}", file=sys.stderr)
    print(
        "\nIf this is a false positive, edit the line so it no longer matches "
        "(use a placeholder like 'sk-test-...' or 'REDACTED'), or whitelist the file in "
        "scripts/check_secrets.py._SKIP_FILES.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
