from __future__ import annotations

from argparse import Namespace

import uvicorn

from ..settings import get_settings

name = "api"


def register(subparsers) -> None:
    subparsers.add_parser(name, help="Serve the FastAPI read API")


def handle(args: Namespace) -> int:
    settings = get_settings()
    uvicorn.run(
        "poochon_backtest_data.api:create_app",
        factory=True,
        host="0.0.0.0",
        port=settings.port,
    )
    return 0
