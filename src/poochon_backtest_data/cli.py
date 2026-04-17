from __future__ import annotations

import logging
import sys

from .commands import build_parser, dispatch
from .settings import get_settings


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    _configure_logging(get_settings().log_level)
    exit_code = dispatch(args) or 0
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
