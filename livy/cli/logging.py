import importlib.util
import logging
import os
import sys
import tempfile
import typing

import livy.cli.config

if typing.TYPE_CHECKING:
    import argparse

_is_initialized = False


def setup_argparse(parser: "argparse.ArgumentParser"):
    group = parser.add_argument_group("Logging")
    cfg = livy.cli.config.load()

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
        help="Silent mode. Only show warning and error log on console.",
    )

    # file
    g = group.add_mutually_exclusive_group()
    g.set_defaults(log_file=cfg.logs.output_file)
    g.add_argument(
        "--log-file",
        metavar="XXXX.log",
        nargs="?",
        dest="log_file",
        help="Output logs into log file. A temporary file would be created if path is not specificied.",
    )
    g.add_argument(
        "--no-log-file",
        action="store_false",
        dest="log_file",
        help="Do not output logs into log file.",
    )
    group.add_argument(
        "--log-file-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=cfg.logs.logfile_level,
        help="Set minimal log level to be written to file. Default: DEBUG.",
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

    # file handler
    if args.log_file or args.log_file is None:
        if not isinstance(args.log_file, str):
            _, path = tempfile.mkstemp(
                prefix="livy-", suffix=".log", dir=os.getcwd(), text=True
            )
            args.log_file = path

        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(_get_general_formatter())
        root_logger.addHandler(file_handler)

    # send init log
    logger = logging.getLogger(__name__)
    logger.debug("Beep- log starts.")
    if args.log_file:
        logger.info("Log file is created at %s", args.log_file)

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
    """Return general formatter. Removed colorlog package specificed format
    syntax before use it."""
    cfg = livy.cli.config.load()
    fmt = cfg.logs.format.replace("%(log_color)s", "").replace("%(reset)s", "")
    return logging.Formatter(fmt=fmt, datefmt=cfg.logs.date_format)


get = logging.getLogger
