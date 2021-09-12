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


class TestArgumentParse(unittest.TestCase):
    def tearDown(self) -> None:
        module._is_initialized = False

    def test_setup_argparse(self):
        p = argparse.ArgumentParser()
        module.setup_argparse(p, True)
        p.parse_args(["-q"])

    def test_init(self):
        args = argparse.Namespace()
        args.verbose = 0
        args.log_file = True

        with tempfile.NamedTemporaryFile() as fp, unittest.mock.patch(
            "os.getcwd", return_value=os.path.dirname(fp.name)
        ):
            module.init(args)

        module.init(args)  # test cache

    def test_init_with_display(self):
        args = argparse.Namespace()
        args.verbose = 0
        args.log_file = True
        args.highlight_logger = ["test-highlight-logger"]
        args.hide_logger = ["test-hide-logger"]

        # fmt: off
        with tempfile.NamedTemporaryFile() as fp, \
            unittest.mock.patch("os.getcwd", return_value=os.path.dirname(fp.name)), \
            unittest.mock.patch("livy.cli.logging._get_console_handler", return_value=[logging.StreamHandler(), "Test message"]):
            module.init(args)
        # fmt: on


class TestFormatter(unittest.TestCase):
    def setUp(self) -> None:
        self.stream = unittest.mock.MagicMock()
        self.stream.isatty.return_value = True

        self.record = logging.makeLogRecord(
            {
                "name": "Test.Bar",
                "levelno": logging.INFO,
                "levelname": "INFO",
                "msg": "Test log message",
                "created": 1631440284,
            }
        )

    @unittest.skipIf(no_colorama, "colorama is not installed")
    def test_get_console_formatter(self):
        assert isinstance(module._get_console_formatter(self.stream), logging.Formatter)

    def test_get_console_formatter_fallback(self):
        self.stream.isatty.return_value = False
        assert isinstance(module._get_console_formatter(self.stream), logging.Formatter)

    @unittest.skipIf(no_colorama, "colorama is not installed")
    @unittest.mock.patch("livy.cli.logging._is_wanted_logger")
    def test_ColoredFormatter(self, is_wanted_logger):
        formatter = module._ColoredFormatter(
            "%(levelcolor)s%(asctime)s %(name)s:%(reset)s %(message)s",
            "%Y-%m-%d %H:%M:%S %z",
        )

        is_wanted_logger.return_value = True
        formatter.format(self.record)

        is_wanted_logger.return_value = False
        formatter.format(self.record)

    def test_is_wanted_logger(self):
        wantted_names = {"Foo.Bar", "Baz"}

        self.record.name = "Foo.Bar"
        assert module._is_wanted_logger(self.record, wantted_names)

        self.record.name = "Foo.Bar.Baz"
        assert module._is_wanted_logger(self.record, wantted_names)

        self.record.name = "Baz"
        assert module._is_wanted_logger(self.record, wantted_names)

        self.record.name = "Baz.Foo"
        assert module._is_wanted_logger(self.record, wantted_names)

        self.record.name = "Foo"
        assert not module._is_wanted_logger(self.record, wantted_names)

        self.record.name = "Foo.BarBaz"
        assert not module._is_wanted_logger(self.record, wantted_names)

        self.record.name = "Qax"
        assert not module._is_wanted_logger(self.record, wantted_names)

    @unittest.mock.patch("livy.cli.logging._console_formatter")
    def test_register_highlight_logger(self, fmt):
        # fail
        module.register_highlight_logger("foo")

        # success
        fmt.highlight_loggers = set()
        module.register_highlight_logger("foo")


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
