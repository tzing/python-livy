import argparse
import decimal
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


class TestGetConsoleHandler(unittest.TestCase):
    def setUp(self) -> None:
        self.stream = unittest.mock.MagicMock()
        self.stream.isatty.return_value = True

    def test_get_console_handler_1(self):
        handler, msg = module._get_console_handler(self.stream, False)
        assert isinstance(handler, logging.Handler)
        assert msg is None

    def test_get_console_handler_2(self):
        self.stream.isatty.return_value = False
        handler, msg = module._get_console_handler(self.stream, True)
        assert isinstance(handler, logging.Handler)
        assert isinstance(msg, str)

    @unittest.mock.patch("importlib.util.find_spec", return_value=None)
    def test_get_console_handler_3(self, _):
        handler, msg = module._get_console_handler(self.stream, True)
        assert isinstance(handler, logging.Handler)
        assert isinstance(msg, str)

    @unittest.mock.patch("importlib.util.find_spec", return_value=True)
    @unittest.mock.patch("livy.cli.logging._StreamHandlerWithProgressbar")
    def test_get_console_handler_4(self, _1, _2):
        _, msg = module._get_console_handler(self.stream, True)
        assert msg is None


@unittest.skipIf(no_tqdm, "tqdm is not installed")
class TestHandler(unittest.TestCase):
    def setUp(self) -> None:
        stream = unittest.mock.MagicMock()
        self.handler = module._StreamHandlerWithProgressbar(stream)

        self.handler.filter = unittest.mock.Mock()

    def record(self, name: str, msg: str) -> logging.LogRecord:
        return logging.makeLogRecord(
            {
                "name": name,
                "levelno": logging.INFO,
                "levelname": "INFO",
                "msg": msg,
                "created": 1631440284,
            }
        )

    def test_handle(self):
        self.handler._set_progressbar = unittest.mock.Mock()
        self.handler._close_progressbar = unittest.mock.Mock()
        self.handler.filter.side_effect = [True, False, True]

        self.handler.handle(
            self.record(
                "YarnScheduler",
                "Adding task set 1.0 with 10 tasks",
            )
        )
        self.handler.handle(
            self.record(
                "TaskSetManager",
                "Finished task 1.0 in stage 1.0 (TID 1) in 0 ms on example.com (executor 2) (1/10)",
            )
        )
        self.handler.handle(
            self.record(
                "YarnScheduler",
                "Removed TaskSet 1.0, whose tasks have all completed, from pool",
            )
        )

    def test_handle_not_related_message(self):
        self.handler._set_progressbar = unittest.mock.Mock()
        self.handler._close_progressbar = unittest.mock.Mock()

        self.handler.handle(self.record("SomeOtherSource", "Not related message"))
        self.handler.handle(self.record("TaskSetManager", "Not related message"))
        self.handler.handle(self.record("YarnScheduler", "Not related message"))

        self.handler._set_progressbar.assert_not_called()
        self.handler._close_progressbar.assert_not_called()

    def test_set_progressbar(self):
        self.handler._close_progressbar = unittest.mock.Mock()

        pb = unittest.mock.MagicMock()
        pb.n = 6
        self.handler._new_tqdm = unittest.mock.Mock(return_value=pb)

        # success
        self.handler._set_progressbar("2.0", 5, 10)
        pb.update.assert_called()

        # failed: older task
        self.handler._set_progressbar("1.0", 9, 10)

        # update
        self.handler._set_progressbar("2.0", 7, 10)
        self.handler._set_progressbar("2.0", 6, 10)

        # new task
        self.handler._set_progressbar("3.0", 7, 10)

        assert pb.update.call_count == 3

    def test_close_progressbar(self):
        # not exists
        self.handler._close_progressbar("1.0")

        # version not match
        self.handler._current_progressbar = pb = unittest.mock.MagicMock()
        self.handler._latest_taskset = decimal.Decimal("1.5")

        self.handler._close_progressbar("1.0")
        pb.close.assert_not_called()

        # closed
        self.handler._close_progressbar("1.5")
        pb.close.assert_called()

    def test_handle_integration(self):
        self.handler.handle(  # new task
            self.record(
                "YarnScheduler",
                "Adding task set 1.0 with 10 tasks",
            )
        )
        self.handler.handle(  # update progress
            self.record(
                "TaskSetManager",
                "Finished task 1.0 in stage 1.0 (TID 1) in 0 ms on example.com (executor 2) (1/10)",
            )
        )
        self.handler.handle(  # new stage
            self.record(
                "TaskSetManager",
                "Finished task 5.0 in stage 3.0 (TID 1) in 0 ms on example.com (executor 2) (3/10)",
            )
        )
        self.handler.handle(  # update progress failed
            self.record(
                "TaskSetManager",
                "Finished task 7.0 in stage 3.0 (TID 1) in 0 ms on example.com (executor 2) (2/10)",
            )
        )
        self.handler.handle(  # new task failed
            self.record(
                "YarnScheduler",
                "Adding task set 2.0 with 10 tasks",
            )
        )
        self.handler.handle(  # remove task failed
            self.record(
                "YarnScheduler",
                "Removed TaskSet 1.0, whose tasks have all completed, from pool",
            )
        )

        assert self.handler._current_progressbar.n == 3
        assert self.handler._latest_taskset == decimal.Decimal("3.0")


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
