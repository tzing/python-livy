import argparse
import logging
import os
import sys
import tempfile
import typing

import livy.cli.config
import livy.utils

_is_initialized = False


def setup_argparse(parser: argparse.ArgumentParser):
    """Setup argparser, in very detail. For use with those complex features like
    submit/read-log."""
    cfg = livy.cli.config.load()

    group = parser.add_argument_group("console")

    # level
    g = group.add_mutually_exclusive_group()
    g.set_defaults(verbose=0)
    g.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="count",
        default=0,
        help="Enable debug log.",
    )
    g.add_argument(
        "-q",
        "--silent",
        dest="verbose",
        action="store_const",
        const=-1,
        help="Silent mode. Only show warning and error log.",
    )

    # highlight and lowlight
    group.add_argument(
        "--highlight-logger",
        metavar="NAME",
        nargs="+",
        default=cfg.logs.highlight_loggers,
        help="Highlight logs from the given loggers. "
        "This option only takes effect when `colorama` is installed.",
    )
    group.add_argument(
        "--hide-logger",
        metavar="NAME",
        nargs="+",
        default=cfg.logs.hide_loggers,
        help="Do not show logs from the given loggers.",
    )

    # progress bar
    g = group.add_mutually_exclusive_group()
    g.set_defaults(with_progressbar=cfg.logs.with_progressbar)
    g.add_argument(
        "--pb",
        "--with-progressbar",
        action="store_true",
        dest="with_progressbar",
        help="Convert TaskSetManager's `Finished task XX in stage Y` logs into progress bar. "
        "This option only takes effect when `tqdm` is installed.",
    )
    g.add_argument(
        "--no-pb",
        "--without-progressbar",
        action="store_false",
        dest="with_progressbar",
        help="Not to convert TaskSetManager's logs into progress bar.",
    )

    group = parser.add_argument_group("file logging")

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


class LoggingArguments(argparse.Namespace):
    verbose: int
    highlight_logger: typing.List[str]
    hide_logger: typing.List[str]
    with_progressbar: bool
    log_file: typing.Union[bool, str]
    log_file_level: int


def init(args: LoggingArguments = None):
    """Initialize loggers"""
    global _is_initialized
    if _is_initialized:
        return

    args = args or argparse.Namespace()
    cfg = livy.cli.config.load()

    # root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.NOTSET)
    logging.captureWarnings(True)

    # console handler
    stream = sys.stderr

    if getattr(args, "with_progressbar", True):
        console_handler = livy.utils.EnhancedConsoleHandler(stream)
    else:
        console_handler = logging.StreamHandler(stream)

    console_log_level = logging.INFO - 10 * getattr(args, "verbose", 0)
    console_handler.setLevel(console_log_level)
    root_logger.addHandler(console_handler)

    console_handler.setFormatter(
        livy.utils.ColoredFormatter(
            fmt=cfg.logs.format,
            datefmt=cfg.logs.date_format,
            highlight_loggers=getattr(args, "highlight_logger", []),
        )
    )

    console_handler.addFilter(
        livy.utils.IngoreLogFilter(getattr(args, "hide_logger", []))
    )

    # file handler
    args.log_file = getattr(args, "log_file", False)
    if args.log_file != False:  # log_file == None is possible
        if not isinstance(args.log_file, str):
            _, path = tempfile.mkstemp(
                prefix="livy-", suffix=".log", dir=os.getcwd(), text=True
            )
            args.log_file = path

        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(_get_general_formatter())
        file_handler.setLevel(getattr(args, "log_file_level", console_log_level))
        root_logger.addHandler(file_handler)

    # send init log
    logger = logging.getLogger(__name__)
    logger.debug("Beep- log starts.")
    if args.log_file:
        logger.info("Log file is created at %s", args.log_file)

    _is_initialized = True


def _get_general_formatter():
    """Return general formatter. Removed color-related syntax before apply."""
    cfg = livy.cli.config.load()
    fmt = cfg.logs.format.replace("%(levelcolor)s", "").replace("%(reset)s", "")
    return logging.Formatter(fmt=fmt, datefmt=cfg.logs.date_format)


get = logging.getLogger
