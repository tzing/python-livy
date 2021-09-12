import contextlib
import decimal
import importlib.util
import logging
import os
import re
import sys
import tempfile
import typing

import livy.cli.config

if typing.TYPE_CHECKING:
    import argparse

_is_initialized = False
_console_formatter = None
_log_filter = None


def setup_argparse(parser: "argparse.ArgumentParser", for_display: bool):
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
    if for_display:
        group.add_argument(
            "--highlight-logger",
            metavar="NAME",
            nargs="+",
            default=[],
            help="Highlight logs from the given loggers. "
            "This option only takes effect when `colorama` is installed.",
        )
        group.add_argument(
            "--hide-logger",
            metavar="NAME",
            nargs="+",
            default=[],
            help="Do not show logs from the given loggers.",
        )

    # progress bar
    if for_display:
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


def init(args: "argparse.Namespace"):
    """Initialize loggers"""
    global _is_initialized, _console_formatter, _log_filter
    if _is_initialized:
        return

    # root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.NOTSET)
    logging.captureWarnings(True)

    # console handler
    stream = sys.stdout

    console_handler, msg_console_handler_create = _get_console_handler(
        stream=stream,
        with_progressbar=getattr(args, "with_progressbar", False),
    )
    console_handler.setLevel(logging.INFO - 10 * args.verbose)
    root_logger.addHandler(console_handler)

    _console_formatter = _get_console_formatter(stream)
    console_handler.setFormatter(_console_formatter)

    _log_filter = _LogFilter()
    console_handler.addFilter(_log_filter)

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

    # buffered message: progress bar handler creation
    if msg_console_handler_create:
        logger.warning(
            "%s. Progressbar feature is disabled.", msg_console_handler_create
        )

    # set highlight / lowlight loggers
    for name in getattr(args, "highlight_logger", []):
        register_highlight_logger(name)
    for name in getattr(args, "hide_logger", []):
        register_ignore_logger(name)

    _is_initialized = True


def _get_console_handler(
    stream: typing.TextIO, with_progressbar: bool
) -> typing.Tuple[logging.StreamHandler, str]:
    """Return fancy console handler if it is wanted and is avaliable."""
    if not with_progressbar:
        return logging.StreamHandler(stream), None
    if not stream.isatty():
        return logging.StreamHandler(stream), "Output stream is not attached to TTY"
    if not importlib.util.find_spec("tqdm"):
        return logging.StreamHandler(stream), "Dependency `tqdm` not found"
    return _StreamHandlerWithProgressbar(stream), None


class _StreamHandlerWithProgressbar(logging.StreamHandler):
    _PATTERN_ADD_TASKSET = re.compile(r"Adding task set ([\d.]+) with (\d+) tasks")
    _PATTERN_REMOVE_TASKSET = re.compile(r"Removed TaskSet ([\d.]+),")
    _PATTERN_FINISH_TASK = re.compile(
        r"Finished task [\d.]+ in stage ([\d.]+) \(.+?\) in \d+ ms on \S+ "
        r"\(executor \d+\) \((\d+)\/(\d+)\)"
    )

    def __init__(self, stream: typing.TextIO) -> None:
        super().__init__(stream)
        import tqdm

        self._current_progressbar: tqdm.tqdm = None
        self._latest_taskset: decimal.Decimal = decimal.Decimal(-1)

        @contextlib.contextmanager
        def lock():
            self.acquire()
            with tqdm.tqdm.external_write_mode():
                yield
            self.release()

        self._writer_lock = lock
        self._new_tqdm = tqdm.tqdm

    def handle(self, record: logging.LogRecord) -> None:
        """Override `handle` for reteriving the log before it is filtered."""
        # capture logs from YarnScheduler / TaskSetManager for updating progressbar
        if record.name == "YarnScheduler":
            msg = record.getMessage()
            m = self._PATTERN_ADD_TASKSET.match(msg)
            if m:
                self._set_progressbar(
                    task_set=m.group(1),
                    progress=0,
                    total=int(m.group(2)),
                )
            else:
                m = self._PATTERN_REMOVE_TASKSET.match(msg)
                if m:
                    self._close_progressbar(m.group(1))

        elif record.name == "TaskSetManager":
            m = self._PATTERN_FINISH_TASK.match(record.getMessage())
            if m:
                self._set_progressbar(
                    task_set=m.group(1),
                    progress=int(m.group(2)),
                    total=int(m.group(3)),
                )

        # filter should be proceed in logging.Handler
        if not self.filter(record):
            return

        # write
        with self._writer_lock():
            self.emit(record)

    def _set_progressbar(self, task_set: str, progress: int, total: int) -> None:
        task_set = decimal.Decimal(task_set)

        # no update by status of older task set
        if task_set < self._latest_taskset:
            return

        # update progress
        elif self._current_progressbar and task_set == self._latest_taskset:
            update = progress - self._current_progressbar.n
            if update > 0:
                self._current_progressbar.update(update)
            return

        # overwrite progressbar for new task set
        elif task_set > self._latest_taskset:
            self._close_progressbar(self._latest_taskset)
            self._latest_taskset = task_set

        # create new progress bar
        self._current_progressbar = self._new_tqdm(
            desc=f"Stage {task_set}",
            total=total,
            leave=True,
        )

        self._current_progressbar.update(progress)

    def _close_progressbar(self, task_set: str) -> bool:
        if not self._current_progressbar:
            return False

        task_set = decimal.Decimal(task_set)
        if task_set != self._latest_taskset:
            return False

        self._current_progressbar.close()
        self._current_progressbar = None
        return True


def _get_general_formatter():
    """Return general formatter. Removed color-related syntax before apply."""
    cfg = livy.cli.config.load()
    fmt = cfg.logs.format.replace("%(levelcolor)s", "").replace("%(reset)s", "")
    return logging.Formatter(fmt=fmt, datefmt=cfg.logs.date_format)


def _get_console_formatter(stream: typing.TextIO):
    """Return colored formatter if avaliable."""
    if not stream.isatty() or not importlib.util.find_spec("colorama"):
        return _get_general_formatter()

    cfg = livy.cli.config.load()
    return _ColoredFormatter(fmt=cfg.logs.format, datefmt=cfg.logs.date_format)


def _is_wanted_logger(record: logging.LogRecord, logger_names: typing.Set[str]) -> bool:
    """Return True if the record is from any of wanted logger."""
    # match by full name
    if record.name in logger_names:
        return True

    # early escape if it could not be a sub logger
    if not record.name or "." not in record.name:
        return False

    # match by logger hierarchy
    for name in logger_names:
        if record.name.startswith(name + "."):
            return True

    return False


class _ColoredFormatter(logging.Formatter):
    """A formatter that could add ANSI colors to logs, should use with console
    stream. Inspired by borntyping/python-colorlog, and add feature that
    supports different color scheme via logger name."""

    __slots__ = ("highlight_loggers",)

    class ColoredRecord:
        def __init__(
            self, record: logging.LogRecord, escapes: typing.Dict[str, str]
        ) -> None:
            self.__dict__.update(record.__dict__)
            self.__dict__.update(escapes)

    _COLOR_RESET: str = ""
    _COLOR_DEFAULT: typing.Dict[str, str] = {}
    _COLOR_HIGHLIGHT: typing.Dict[str, str] = {}

    def __init__(self, fmt: str, datefmt: str) -> None:
        """Create ColorFormatter instance"""
        super().__init__(fmt=fmt, datefmt=datefmt)
        import colorama

        colorama.init(strip=True)

        self._COLOR_RESET = colorama.Style.RESET_ALL
        self._COLOR_DEFAULT = {
            "DEBUG": colorama.Fore.WHITE,
            "INFO": colorama.Fore.GREEN,
            "WARNING": colorama.Fore.YELLOW,
            "ERROR": colorama.Fore.RED,
            "CRITICAL": colorama.Fore.LIGHTRED_EX,
        }
        self._COLOR_HIGHLIGHT = {
            "DEBUG": colorama.Back.WHITE + colorama.Fore.WHITE,
            "INFO": colorama.Back.GREEN + colorama.Fore.WHITE,
            "WARNING": colorama.Back.YELLOW + colorama.Fore.WHITE,
            "ERROR": colorama.Back.RED + colorama.Fore.WHITE,
            "CRITICAL": colorama.Back.RED + colorama.Fore.WHITE,
        }

        self.highlight_loggers = set()

    def formatMessage(self, record: logging.LogRecord) -> str:
        colors = self.get_color_map(record)
        wrapper = self.ColoredRecord(record, colors)
        message = super().formatMessage(wrapper)
        if not message.endswith(self._COLOR_RESET):
            message += self._COLOR_RESET
        return message

    def get_color_map(self, record: logging.LogRecord) -> typing.Dict[str, str]:
        colors = {
            "reset": self._COLOR_RESET,
        }

        if _is_wanted_logger(record, self.highlight_loggers):
            colors["levelcolor"] = self._COLOR_HIGHLIGHT.get(
                record.levelname, self._COLOR_RESET
            )
        else:
            colors["levelcolor"] = self._COLOR_DEFAULT.get(
                record.levelname, self._COLOR_RESET
            )

        return colors


def register_highlight_logger(name: str):
    """Register logger name to be highlighted on console.

    Parameters
    ----------
        name : str
            Logger name
    """
    global _console_formatter
    assert isinstance(name, str)
    assert _console_formatter, "Console formatter does not exist"

    if not hasattr(_console_formatter, "highlight_loggers"):
        get(__name__).warning(
            "Log highlighting feature is currently not avaliable. "
            "Do you have python library `colorama` installed?",
        )
        return

    _console_formatter.highlight_loggers.add(name)


class _LogFilter(object):
    """Drop logs from the unwanted logger list."""

    __slots__ = ("unwanted_loggers",)

    def __init__(self) -> None:
        self.unwanted_loggers = set()

    def filter(self, record: logging.LogRecord) -> bool:
        """Determine if the specified record is to be logged.
        Returns True if the record should be logged, or False otherwise.
        """
        if _is_wanted_logger(record, self.unwanted_loggers):
            return False
        return True


def register_ignore_logger(name: str):
    """Register logger name to be ignored on console.

    Parameters
    ----------
        name : str
            Logger name
    """
    global _log_filter
    assert isinstance(name, str)
    assert _log_filter, "Log filter does not exists"

    _log_filter.unwanted_loggers.add(name)


get = logging.getLogger
