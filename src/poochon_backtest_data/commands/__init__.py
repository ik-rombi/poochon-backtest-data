from __future__ import annotations

from argparse import ArgumentParser, Namespace
from typing import Protocol


class CommandModule(Protocol):
    name: str

    def register(self, subparsers) -> None: ...

    def handle(self, args: Namespace) -> int: ...


def build_parser() -> ArgumentParser:
    from . import data, infra, job, run, schedule, submit

    parser = ArgumentParser(prog="poochon-backtest-data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    modules = [infra, data, run, submit, schedule, job]
    dispatch_table: dict[str, CommandModule] = {}
    for module in modules:
        module.register(subparsers)
        dispatch_table[module.name] = module

    parser.set_defaults(_modules=dispatch_table)
    return parser


def dispatch(args: Namespace) -> int:
    modules = getattr(args, "_modules", {})
    module = modules.get(args.command)
    if module is None:
        raise RuntimeError(f"unknown command: {args.command}")
    return module.handle(args)
