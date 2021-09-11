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
    module.setup_argparse(p)
    p.parse_args(["-q"])


@unittest.mock.patch("livy.cli.logging._is_initialized", False)
def test_init():
    args = argparse.Namespace()
    args.verbose = 0
    args.log_file = True
    args.highlight_logger = ["test-logger"]

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
def record():
    return logging.makeLogRecord(
        {
            "name": "Test.Bar",
            "levelno": logging.INFO,
            "levelname": "INFO",
            "msg": "Test log message",
            "created": (time.time()),
        }
    )


def test__ColoredFormatter(record):
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


def test__is_wanted_logger(record):
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
