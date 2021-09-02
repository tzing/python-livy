import argparse
import logging

_is_initialized = False


def setup_argparse(parser: argparse.ArgumentParser):
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
        help="Enable debug log",
    )
    g.add_argument(
        "-q",
        "--silent",
        dest="verbose",
        action="store_const",
        const=-1,
        help="Silent mode. only show warning and error log.",
    )


def init(args: argparse.Namespace):
    """Initialize loggers"""
    global _is_initialized
    if _is_initialized:
        return

    level = logging.INFO - 10 * args.verbose

    logging.basicConfig(level=level)

    _is_initialized = True


def get(name="main"):
    """Get logger"""
    return logging.getLogger(name)
