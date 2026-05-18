"""CLI surface for vectorwave-check: `vectorwave check <subcommand>`."""
from __future__ import annotations

import argparse
import sys
from typing import Optional


def _cmd_calibrate(args: argparse.Namespace) -> int:
    from .calibrate import calibrate, format_report

    try:
        result = calibrate(
            args.target,
            rerun=args.rerun,
            samples=args.samples,
            runs=args.runs,
        )
    except RuntimeError as e:
        print(f"calibrate: {e}", file=sys.stderr)
        return 1

    print(format_report(result))
    return 0


def add_check_subparser(subparsers: "argparse._SubParsersAction") -> None:
    check = subparsers.add_parser(
        "check",
        help="Semantic regression testing tools (calibration, discovery)",
    )
    check_sub = check.add_subparsers(dest="check_cmd", required=True)

    calibrate_p = check_sub.add_parser(
        "calibrate",
        help="Compute a recommended similarity threshold for a target function",
    )
    calibrate_p.add_argument(
        "target",
        help="Fully-qualified function name, e.g. 'myapp.summarize'",
    )
    calibrate_p.add_argument(
        "--rerun",
        action="store_true",
        help=(
            "Re-execute the function on sampled inputs to measure intrinsic "
            "noise floor. Default mode only reads existing golden outputs."
        ),
    )
    calibrate_p.add_argument(
        "--samples",
        type=int,
        default=None,
        help=(
            "Number of goldens (diversity mode) / inputs (rerun mode) to use. "
            "Defaults: 30 (diversity), 3 (rerun)."
        ),
    )
    calibrate_p.add_argument(
        "--runs",
        type=int,
        default=10,
        help="Re-executions per sampled input. Only used with --rerun. Default: 10.",
    )
    calibrate_p.set_defaults(func=_cmd_calibrate)


def main(argv: Optional[list] = None) -> int:
    """Entry point for direct invocation (e.g. `python -m vectorwave.check.cli ...`)."""
    parser = argparse.ArgumentParser(prog="vectorwave-check")
    sub = parser.add_subparsers(dest="cmd", required=True)
    add_check_subparser(sub)
    args = parser.parse_args(argv)
    return args.func(args) or 0
