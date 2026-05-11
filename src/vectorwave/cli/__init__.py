"""VectorWave CLI entry point.

`vectorwave dev` manages a local Weaviate instance for running test_ex/ scripts
end-to-end without manually wiring docker-compose, env vars, and the Rust build.
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from importlib import resources
from pathlib import Path

PROJECT_NAME = "vectorwave-dev"
WEAVIATE_HOST = "localhost"
WEAVIATE_HTTP_PORT = 8080
WEAVIATE_GRPC_PORT = 50051
WEAVIATE_READY_URL = f"http://{WEAVIATE_HOST}:{WEAVIATE_HTTP_PORT}/v1/.well-known/ready"
WEAVIATE_READY_TIMEOUT_S = 60


def _compose_path() -> Path:
    return resources.files("vectorwave.cli.dev").joinpath("compose.yml")


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(
        ["docker", "info"], capture_output=True
    ).returncode == 0


def _compose(*args: str, check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT_NAME, "-f", str(_compose_path()), *args],
        check=check,
    )


def _wait_until_ready(timeout: int = WEAVIATE_READY_TIMEOUT_S) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(WEAVIATE_READY_URL, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(1)
    return False


def _maturin_build_warning() -> None:
    try:
        import vectorwave.vectorwave_core  # noqa: F401
    except ImportError:
        print(
            "[warn] Rust extension not compiled. Run 'maturin develop' to enable "
            "the high-performance batch path (Python fallback will be used until then).",
            file=sys.stderr,
        )


def cmd_start(_args: argparse.Namespace) -> int:
    if not _docker_available():
        print("[error] Docker is not available. Install/start Docker first.", file=sys.stderr)
        return 1
    print(f"[start] bringing up '{PROJECT_NAME}' stack")
    rc = _compose("up", "-d").returncode
    if rc != 0:
        return rc
    print("[start] waiting for Weaviate to be ready...")
    if not _wait_until_ready():
        print("[error] Weaviate did not become ready in time. Check 'vectorwave dev logs'.", file=sys.stderr)
        return 1
    _maturin_build_warning()
    print("\n  Weaviate : http://localhost:8080")
    print("  gRPC     : localhost:50051")
    print("  Console  : http://localhost:8081\n")
    print("Set in your .env (or shell) to point scripts at this instance:")
    print("  WEAVIATE_HOST=localhost")
    print("  WEAVIATE_PORT=8080")
    print("  WEAVIATE_GRPC_PORT=50051")
    return 0


def cmd_stop(_args: argparse.Namespace) -> int:
    return _compose("down").returncode


def cmd_reset(_args: argparse.Namespace) -> int:
    rc = _compose("down", "-v").returncode
    if rc != 0:
        return rc
    return cmd_start(_args)


def cmd_status(_args: argparse.Namespace) -> int:
    rc = _compose("ps").returncode
    try:
        with urllib.request.urlopen(WEAVIATE_READY_URL, timeout=2) as resp:
            print(f"weaviate ready endpoint: HTTP {resp.status}")
    except Exception as e:
        print(f"weaviate ready endpoint: unreachable ({e})")
    return rc


def cmd_logs(args: argparse.Namespace) -> int:
    compose_args = ["logs"]
    if args.follow:
        compose_args.append("-f")
    else:
        compose_args.extend(["--tail", str(args.tail)])
    if args.service:
        compose_args.append(args.service)
    return _compose(*compose_args).returncode


def _check_running() -> bool:
    try:
        with urllib.request.urlopen(WEAVIATE_READY_URL, timeout=2) as resp:
            return resp.status == 200
    except Exception:
        return False


def cmd_shell(args: argparse.Namespace) -> int:
    """Drop into a subshell with WEAVIATE_* env vars exported."""
    if not _check_running():
        print(
            "[error] Weaviate is not reachable at "
            f"{WEAVIATE_READY_URL}. Run 'vectorwave dev start' first.",
            file=sys.stderr,
        )
        return 1
    env = os.environ.copy()
    env["WEAVIATE_HOST"] = WEAVIATE_HOST
    env["WEAVIATE_PORT"] = str(WEAVIATE_HTTP_PORT)
    env["WEAVIATE_GRPC_PORT"] = str(WEAVIATE_GRPC_PORT)
    env.setdefault("VECTORWAVE_DEV_SHELL", "1")
    shell = args.shell or env.get("SHELL") or "/bin/bash"
    print(f"[shell] starting {shell} with WEAVIATE_HOST/PORT/GRPC_PORT exported")
    print("[shell]   exit or Ctrl-D to leave")
    return subprocess.run([shell], env=env).returncode


def cmd_seed(_args: argparse.Namespace) -> int:
    """Seed a small set of sample functions + executions for demo / smoke testing."""
    if not _check_running():
        print(
            "[error] Weaviate is not reachable at "
            f"{WEAVIATE_READY_URL}. Run 'vectorwave dev start' first.",
            file=sys.stderr,
        )
        return 1

    os.environ.setdefault("WEAVIATE_HOST", WEAVIATE_HOST)
    os.environ.setdefault("WEAVIATE_PORT", str(WEAVIATE_HTTP_PORT))
    os.environ.setdefault("WEAVIATE_GRPC_PORT", str(WEAVIATE_GRPC_PORT))
    # Demo data uses no vectorizer so the seed runs without an OpenAI key.
    os.environ["VECTORIZER"] = "none"
    os.environ["IS_VECTORIZE_COLLECTION_NAME"] = "False"

    try:
        from vectorwave.database.db import (
            create_execution_schema,
            create_vectorwave_schema,
            get_weaviate_client,
        )
        from vectorwave.models.db_config import get_weaviate_settings
    except ImportError as e:
        print(f"[error] failed to import vectorwave: {e}", file=sys.stderr)
        return 1

    # Reset cached singletons so the seeded settings pick up fresh env vars.
    from vectorwave.models.db_config import get_weaviate_settings as _gws
    if hasattr(_gws, "cache_clear"):
        _gws.cache_clear()

    settings = get_weaviate_settings()
    client = get_weaviate_client(settings)
    try:
        # Recreate collections with vectorizer=none so the seed is self-contained.
        for name in (settings.COLLECTION_NAME, settings.EXECUTION_COLLECTION_NAME):
            if client.collections.exists(name):
                client.collections.delete(name)
        create_vectorwave_schema(client, settings)
        create_execution_schema(client, settings)

        funcs = client.collections.get(settings.COLLECTION_NAME)
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)

        from datetime import datetime, timezone
        from uuid import uuid4

        now = datetime.now(timezone.utc).isoformat()
        sample_funcs = [
            {"function_name": "calculate_total", "module_name": "demo.billing",
             "file_path": "demo/billing.py", "docstring": "Sum line items with tax",
             "source_code": "def calculate_total(items, rate):\n    return sum(i*rate for i in items)\n",
             "search_description": "Compute total price with tax rate",
             "sequence_narrative": "Iterates items, multiplies each by rate, sums."},
            {"function_name": "render_card", "module_name": "demo.ui",
             "file_path": "demo/ui.py", "docstring": "Render a card",
             "source_code": "def render_card(props):\n    return f'<div>{props}</div>'\n",
             "search_description": "Render a UI card from props",
             "sequence_narrative": "Wraps props in a div tag."},
        ]
        for props in sample_funcs:
            funcs.data.insert(properties=props)

        sample_execs = [
            {"function_uuid": str(uuid4()), "function_name": "calculate_total",
             "status": "SUCCESS", "duration_ms": 12.5, "timestamp_utc": now,
             "return_value": "42.0"},
            {"function_uuid": str(uuid4()), "function_name": "calculate_total",
             "status": "ERROR", "duration_ms": 3.2, "timestamp_utc": now,
             "error_message": "TypeError: unsupported operand", "error_code": "TypeError"},
            {"function_uuid": str(uuid4()), "function_name": "render_card",
             "status": "SUCCESS", "duration_ms": 0.4, "timestamp_utc": now,
             "return_value": "'<div>{}</div>'"},
        ]
        for props in sample_execs:
            execs.data.insert(properties=props)

        print(f"[seed] inserted {len(sample_funcs)} functions and {len(sample_execs)} execution logs")
        print("       try: vectorwave dev shell  →  python -c 'from vectorwave.search import ...'")
    finally:
        client.close()
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vectorwave")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    dev = subparsers.add_parser("dev", help="Manage the local e2e dev environment")
    dev_sub = dev.add_subparsers(dest="dev_cmd", required=True)

    dev_sub.add_parser("start", help="Start Weaviate + console").set_defaults(func=cmd_start)
    dev_sub.add_parser("stop", help="Stop the dev stack").set_defaults(func=cmd_stop)
    dev_sub.add_parser("reset", help="Wipe data volumes and restart").set_defaults(func=cmd_reset)
    dev_sub.add_parser("status", help="Show stack status").set_defaults(func=cmd_status)

    logs = dev_sub.add_parser("logs", help="Tail container logs")
    logs.add_argument("service", nargs="?", default=None, help="Service name (default: all)")
    logs.add_argument("-f", "--follow", action="store_true")
    logs.add_argument("-n", "--tail", type=int, default=100)
    logs.set_defaults(func=cmd_logs)

    shell = dev_sub.add_parser("shell", help="Drop into a subshell with WEAVIATE_* env vars exported")
    shell.add_argument("--shell", help="Shell binary to invoke (default: $SHELL or /bin/bash)")
    shell.set_defaults(func=cmd_shell)

    dev_sub.add_parser("seed", help="Insert a small demo dataset of functions + execution logs").set_defaults(func=cmd_seed)

    subparsers.add_parser(
        "info",
        help="List Python processes currently running with VectorWave imported",
    ).set_defaults(func=cmd_info)

    return parser


def cmd_info(_args: argparse.Namespace) -> int:
    """Print a table of every live process that has VectorWave active.

    Reads PID files written by ``vectorwave.runtime.activate`` and prunes
    stale entries (PIDs that aren't alive any more).
    """
    from vectorwave.runtime import list_active_processes

    # Exclude the CLI's own PID — it's only "active" because importing
    # vectorwave to run this command activated the indicator.
    procs = [p for p in list_active_processes() if p.pid != os.getpid()]
    if not procs:
        print("[vectorwave info] no active VectorWave processes found.")
        return 0

    now = time.time()
    rows = []
    header = ("PID", "MODE", "OTEL", "RUST", "AGE", "MODULES")
    for p in procs:
        age = int(now - (p.started_at or now))
        age_str = f"{age // 3600}h{(age % 3600) // 60}m" if age >= 3600 else f"{age // 60}m{age % 60}s"
        otel = f"on:{p.otel_service_name}" if p.otel_enabled else "off"
        rust = "yes" if p.rust_core else "py"
        mods = ",".join(p.instrumented_modules) or "(decorator)"
        if len(mods) > 50:
            mods = mods[:47] + "..."
        rows.append((str(p.pid), p.mode, otel, rust, age_str, mods))

    widths = [max(len(r[i]) for r in (rows + [header])) for i in range(len(header))]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*header))
    print(fmt.format(*("-" * w for w in widths)))
    for row in rows:
        print(fmt.format(*row))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
