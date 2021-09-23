import argparse
import importlib.util
import logging
import os
import tempfile
import unittest
import unittest.mock

import livy.cli.logging as module

# some package are optional; they are not installed in `test-core`
no_colorama = importlib.util.find_spec("colorama") is None
no_tqdm = importlib.util.find_spec("tqdm") is None


class TestArgumentParse(unittest.TestCase):
    def tearDown(self) -> None:
        module._is_initialized = False

    def test_setup_argparse(self):
        p = argparse.ArgumentParser()
        module.setup_argparse(p)
        p.parse_args(["-q"])

    def test_init(self):
        with tempfile.NamedTemporaryFile() as fp, unittest.mock.patch(
            "os.getcwd", return_value=os.path.dirname(fp.name)
        ):
            module.init()

        module.init()  # test cache

    def test_init_with_display(self):
        args = argparse.Namespace()
        args.verbose = 0
        args.log_file = True
        args.highlight_logger = ["test-highlight-logger"]
        args.hide_logger = ["test-hide-logger"]
        args.with_progressbar = False

        # fmt: off
        with tempfile.NamedTemporaryFile() as fp, \
            unittest.mock.patch("os.getcwd", return_value=os.path.dirname(fp.name)):
            module.init(args)
        # fmt: on


class TestFilter(unittest.TestCase):
    def setUp(self) -> None:
        self.record = logging.makeLogRecord(
            {
                "name": "Test.Bar",
                "levelno": logging.INFO,
                "levelname": "INFO",
                "msg": "Test log message",
                "created": 1631440284,
            }
        )

        self.filter = module._LogFilter()
        self.filter.unwanted_loggers.add("Foo.Bar")

    def test_pass(self):
        self.record.name = "Test"
        assert self.filter.filter(self.record)

        self.record.name = "Foo.Baz"
        assert self.filter.filter(self.record)

        self.record.name = "Foo.BarBaz"
        assert self.filter.filter(self.record)

    def test_fail(self):
        self.record.name = "Foo.Bar"
        assert not self.filter.filter(self.record)

        self.record.name = "Foo.Bar.Baz"
        assert not self.filter.filter(self.record)
