import importlib.util
import logging
import sys
import typing

import livy.cli.config

if typing.TYPE_CHECKING:
    import argparse

_is_initialized = False


def setup_argparse(parser: "argparse.ArgumentParser"):
    group = parser.add_argument_group("logging")

    # level
    g = group.add_mutually_exclusive_group()
    g.set_defaults(verbose=0)
    g.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="count",
        default=0,
        help="Enable debug log on console.",
    )
    g.add_argument(
        "-q",
        "--silent",
        dest="verbose",
        action="store_const",
        const=-1,
        help="Silent mode. Only show warning and error log.",
    )


def init(args: "argparse.Namespace"):
    """Initialize loggers"""
    global _is_initialized
    if _is_initialized:
        return

    # root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.NOTSET)
    logging.captureWarnings(True)

    # console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO - 10 * args.verbose)
    console_handler.setFormatter(_get_console_formatter())
    root_logger.addHandler(console_handler)

    _is_initialized = True


def _use_color_handler():
    """Return true if `colorlog` is installed and tty is attached."""
    return sys.stdout.isatty() and importlib.util.find_spec("colorlog")


def _get_console_formatter():
    """Return colored formatter if avaliable."""
    if not _use_color_handler():
        return _get_general_formatter()

    import colorlog

    cfg = livy.cli.config.load()
    return colorlog.ColoredFormatter(fmt=cfg.logs.format, datefmt=cfg.logs.date_format)


def _get_general_formatter():
    cfg = livy.cli.config.load()
    fmt = cfg.logs.format.replace("%(log_color)s", "").replace("%(reset)s", "")
    return logging.Formatter(fmt=fmt, datefmt=cfg.logs.date_format)


get = logging.getLogger
