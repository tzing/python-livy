import argparse
import importlib.util
import logging
import os
import tempfile
import time
import unittest.mock

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

    formatter.highlight_loggers.add("Test.Foo")

    # default
    formatter.format(
        logging.makeLogRecord(
            {
                "name": "Test.Bar",
                "levelno": logging.INFO,
                "levelname": "INFO",
                "msg": "Test log message",
                "created": (time.time()),
            }
        )
    )

    # highlight, exact match
    formatter.format(
        logging.makeLogRecord(
            {
                "name": "Test.Foo",
                "levelno": logging.INFO,
                "levelname": "INFO",
                "msg": "Test log message",
                "created": (time.time()),
            }
        )
    )

    # highlight, sub logger
    formatter.format(
        logging.makeLogRecord(
            {
                "name": "Test.Foo.Baz",
                "levelno": logging.INFO,
                "levelname": "INFO",
                "msg": "Test log message",
                "created": (time.time()),
            }
        )
    )


@unittest.mock.patch("livy.cli.logging._use_color_handler", return_value=False)
def test__get_console_formatter_fallback(_):
    assert isinstance(module._get_console_formatter(), logging.Formatter)
