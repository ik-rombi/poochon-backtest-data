from __future__ import annotations

from argparse import ArgumentParser, Namespace
from typing import Protocol


class CommandModule(Protocol):
    name: str

    def register(self, subparsers) -> None: ...

    def handle(self, args: Namespace) -> int: ...


def build_parser() -> ArgumentParser:
    from . import api, data, infra, job, legacy, run, submit

    parser = ArgumentParser(prog="poochon-backtest-data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    primary_modules = [api, infra, data, run, submit, job]
    dispatch_table: dict[str, CommandModule] = {}
    for module in primary_modules:
        module.register(subparsers)
        dispatch_table[module.name] = module

    legacy.register(subparsers)
    for command_name in legacy.legacy_command_names():
        dispatch_table[command_name] = legacy

    parser.set_defaults(_modules=dispatch_table)
    return parser


def dispatch(args: Namespace) -> int:
    modules = getattr(args, "_modules", {})
    module = modules.get(args.command)
    if module is None:
        raise RuntimeError(f"unknown command: {args.command}")
    return module.handle(args)
