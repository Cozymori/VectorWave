"""VectorWave CLI entry point.

`vectorwave dev` manages a local Weaviate instance for running test_ex/ scripts
end-to-end without manually wiring docker-compose, env vars, and the Rust build.
"""
import argparse
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from importlib import resources
from pathlib import Path

PROJECT_NAME = "vectorwave-dev"
WEAVIATE_READY_URL = "http://localhost:8080/v1/.well-known/ready"
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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
