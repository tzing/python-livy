import argparse
import decimal
import importlib.util
import logging
import os
import tempfile
import time
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
        import colorama

        # dot not include time, local machine is not in utc timezone
        formatter = module._ColoredFormatter(
            "%(levelcolor)s%(asctime)s %(name)s:%(reset)s %(message)s",
            "%Y-%m-%d",
        )

        # highlight
        is_wanted_logger.return_value = True
        self.assertEqual(
            formatter.format(self.record),
            f"{colorama.Back.GREEN}{colorama.Fore.WHITE}"
            "2021-09-12 Test.Bar:"
            f"{colorama.Style.RESET_ALL}"
            " Test log message"
            f"{colorama.Style.RESET_ALL}",
        )

        # normal
        is_wanted_logger.return_value = False
        self.assertEqual(
            formatter.format(self.record),
            f"{colorama.Fore.GREEN}"
            "2021-09-12 Test.Bar:"
            f"{colorama.Style.RESET_ALL}"
            " Test log message"
            f"{colorama.Style.RESET_ALL}",
        )

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
