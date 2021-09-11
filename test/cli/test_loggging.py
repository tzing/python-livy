import argparse
import importlib.util
import logging
import os
import tempfile
import time
import unittest.mock

import pytest

import livy.cli.logging as module


def test_setup_argparse():
    p = argparse.ArgumentParser()
    module.setup_argparse(p, True)
    p.parse_args(["-q"])


@pytest.mark.parametrize("with_display_feature", [True, False])
@unittest.mock.patch("livy.cli.logging._is_initialized", False)
def test_init_with_display(with_display_feature):
    args = argparse.Namespace()
    args.verbose = 0
    args.log_file = True

    if with_display_feature:
        args.highlight_logger = ["test-highlight-logger"]
        args.hide_logger = ["test-hide-logger"]

    with tempfile.NamedTemporaryFile() as fp, unittest.mock.patch(
        "os.getcwd", return_value=os.path.dirname(fp.name)
    ):
        module.init(args)

    module.init(args)


@unittest.mock.patch("livy.cli.logging._use_color_handler", return_value=True)
def test__get_console_formatter_colored(_):
    if not importlib.util.find_spec("colorama"):  # test-core does not install colorlog
        return

    formatter = module._get_console_formatter()
    assert isinstance(formatter, logging.Formatter)


@pytest.fixture
def record() -> logging.LogRecord:
    return logging.makeLogRecord(
        {
            "name": "Test.Bar",
            "levelno": logging.INFO,
            "levelname": "INFO",
            "msg": "Test log message",
            "created": (time.time()),
        }
    )


def test__ColoredFormatter(record: logging.LogRecord):
    if not importlib.util.find_spec("colorama"):  # test-core does not install colorlog
        return

    formatter = module._ColoredFormatter(
        "%(levelcolor)s%(asctime)s %(name)s:%(reset)s %(message)s",
        "%Y-%m-%d %H:%M:%S %z",
    )

    with unittest.mock.patch("livy.cli.logging._is_wanted_logger", return_value=True):
        formatter.format(record)

    with unittest.mock.patch("livy.cli.logging._is_wanted_logger", return_value=False):
        formatter.format(record)


def test__is_wanted_logger(record: logging.LogRecord):
    wantted_names = {"Foo.Bar", "Baz"}

    record.name = "Foo.Bar"
    assert module._is_wanted_logger(record, wantted_names)

    record.name = "Foo.Bar.Baz"
    assert module._is_wanted_logger(record, wantted_names)

    record.name = "Baz"
    assert module._is_wanted_logger(record, wantted_names)

    record.name = "Baz.Foo"
    assert module._is_wanted_logger(record, wantted_names)

    record.name = "Foo"
    assert not module._is_wanted_logger(record, wantted_names)

    record.name = "Foo.BarBaz"
    assert not module._is_wanted_logger(record, wantted_names)

    record.name = "Qax"
    assert not module._is_wanted_logger(record, wantted_names)


@unittest.mock.patch("livy.cli.logging._use_color_handler", return_value=False)
def test__get_console_formatter_fallback(_):
    assert isinstance(module._get_console_formatter(), logging.Formatter)


@unittest.mock.patch("livy.cli.logging._console_formatter")
def test_register_highlight_logger(fmt):
    # fail
    module.register_highlight_logger("foo")

    # success
    fmt.highlight_loggers = set()
    module.register_highlight_logger("foo")


def test__LogFilter(record: logging.LogRecord):
    filter_ = module._LogFilter()
    filter_.unwanted_loggers.add("Foo")

    record.name = "Test"
    assert filter_.filter(record)

    record.name = "Foo.Bar"
    assert not filter_.filter(record)
