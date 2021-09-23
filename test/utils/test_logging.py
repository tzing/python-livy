import decimal
import importlib
import logging
import sys
import time
import unittest
import unittest.mock

import livy.utils.logging as module


# optional package are not installed in `test-core`
no_colorama = importlib.util.find_spec("colorama") is None
no_tqdm = importlib.util.find_spec("tqdm") is None


class EnhancedConsoleHandlerSwitchTester(unittest.TestCase):
    """For testing EnhancedConsoleHandler's __new__"""

    @unittest.mock.patch("importlib.util.find_spec", side_effect=ImportError())
    def test_without_tqdm(self, _):
        handler = module.EnhancedConsoleHandler(sys.stdout)
        self.assertIsInstance(handler, logging.StreamHandler)
        self.assertNotIsInstance(handler, module.EnhancedConsoleHandler)

    @unittest.skipIf(no_tqdm, "tqdm is not installed")
    @unittest.mock.patch("importlib.util.find_spec")
    def test_with_tqdm(self, _):
        stream = unittest.mock.MagicMock()
        handler = module.EnhancedConsoleHandler(stream)
        self.assertIsInstance(handler, module.EnhancedConsoleHandler)


@unittest.skipIf(no_tqdm, "tqdm is not installed")
class EnhancedConsoleHandlerTester(unittest.TestCase):
    def setUp(self) -> None:
        stream = unittest.mock.MagicMock()
        self.handler = module.EnhancedConsoleHandler(stream)
        self.handler.filter = unittest.mock.Mock()

    def tearDown(self) -> None:
        self.handler.close()

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
        self.handler._tqdm_create = unittest.mock.Mock(return_value=pb)

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

    def test_flush_no_record(self):
        self.handler.flush()

    def test_flush_by_thread(self):
        self.handler.emit = unittest.mock.MagicMock()

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

        time.sleep(0.3)  # wait for background thread
        assert self.handler.emit.call_count == 6

    def test_sink(self):
        self.handler.close()
        self.handler.handle(self.record("Test", "Test log"))
        self.handler.flush()


class ColoredFormatterSwitchTester(unittest.TestCase):
    """For testing ColoredFormatter's __new__"""

    @unittest.skipIf(no_colorama, "colorama is not installed")
    @unittest.mock.patch("sys.stdout")
    def test_avaliable(self, _):
        formatter = module.ColoredFormatter(
            "%(levelcolor)s%(asctime)s %(name)s:%(reset)s %(message)s",
            "%Y-%m-%d",
        )
        self.assertIsInstance(formatter, module.ColoredFormatter)

    @unittest.mock.patch("sys.stdout")
    def test_not_avaliable(self, stdout):
        stdout.isatty.return_value = False

        # create instance
        formatter = module.ColoredFormatter(
            "%(levelcolor)s%(asctime)s %(name)s:%(reset)s %(message)s",
            "%Y-%m-%d",
        )
        self.assertIsInstance(formatter, logging.Formatter)
        self.assertNotIsInstance(formatter, module.ColoredFormatter)

        # ensure color is removed
        self.assertEqual(
            formatter.format(
                logging.makeLogRecord(
                    {
                        "name": "Test",
                        "levelno": logging.INFO,
                        "levelname": "INFO",
                        "msg": "Test log message",
                        "created": 1631440284,
                    }
                )
            ),
            "2021-09-12 Test: Test log message",
        )


@unittest.skipIf(no_colorama, "colorama is not installed")
class ColoredFormatterTester(unittest.TestCase):
    @unittest.mock.patch("sys.stdout")
    def setUp(self, _) -> None:
        # dot not include time, local machine is not in utc timezone
        self.formatter = module.ColoredFormatter(
            "%(levelcolor)s%(asctime)s %(name)s:%(reset)s %(message)s",
            "%Y-%m-%d",
        )

    def test_formatMessage(self):
        import colorama

        self.formatter.get_color_map = unittest.mock.Mock(
            return_value={
                "levelcolor": colorama.Fore.RED,
                "reset": colorama.Style.RESET_ALL,
            }
        )

        self.assertEqual(
            self.formatter.format(
                logging.makeLogRecord(
                    {
                        "name": "Test",
                        "levelno": logging.INFO,
                        "levelname": "INFO",
                        "msg": "Test log message",
                        "created": 1631440284,
                    }
                )
            ),
            f"{colorama.Fore.RED}"
            "2021-09-12 Test:"
            f"{colorama.Style.RESET_ALL}"
            " Test log message"
            f"{colorama.Style.RESET_ALL}",
        )

    @unittest.mock.patch("livy.utils.logging.is_from_wanted_logger")
    def test_get_color_map(self, ifwl):
        import colorama

        record = unittest.mock.Mock(spec=logging.LogRecord)
        record.levelname = "INFO"

        # highlight
        ifwl.return_value = True
        self.assertDictEqual(
            self.formatter.get_color_map(record),
            {
                "levelcolor": colorama.Back.GREEN
                + colorama.Style.BRIGHT
                + colorama.Fore.WHITE,
                "reset": colorama.Style.RESET_ALL,
            },
        )

        # normal
        ifwl.return_value = False
        self.assertDictEqual(
            self.formatter.get_color_map(record),
            {
                "levelcolor": colorama.Fore.GREEN,
                "reset": colorama.Style.RESET_ALL,
            },
        )


class IngoreLogFilterTester(unittest.TestCase):
    def setUp(self) -> None:
        self.filter = module.IngoreLogFilter(["Test.Foo"])

    def record(self, name: str) -> logging.LogRecord:
        return logging.makeLogRecord(
            {
                "name": name,
                "levelno": logging.INFO,
                "levelname": "INFO",
                "msg": "Test log message",
                "created": 1632401177,
            }
        )

    def test(self):
        self.assertTrue(self.filter.filter(self.record("Test")))
        self.assertTrue(self.filter.filter(self.record("Test.Bar")))
        self.assertTrue(self.filter.filter(self.record("TestFoo")))

        self.assertFalse(self.filter.filter(self.record("Test.Foo")))
        self.assertFalse(self.filter.filter(self.record("Test.Foo.Bar")))


class HelperTester(unittest.TestCase):
    def test_is_from_wanted_logger(self):
        record = unittest.mock.Mock(spec=logging.LogRecord)
        wantted_names = {"Foo.Bar", "Baz"}

        record.name = "Foo.Bar"
        self.assertTrue(module.is_from_wanted_logger(wantted_names, record))

        record.name = "Foo.Bar.Baz"
        self.assertTrue(module.is_from_wanted_logger(wantted_names, record))

        record.name = "Baz"
        self.assertTrue(module.is_from_wanted_logger(wantted_names, record))

        record.name = "Baz.Foo"
        self.assertTrue(module.is_from_wanted_logger(wantted_names, record))

        record.name = "Foo"
        self.assertFalse(module.is_from_wanted_logger(wantted_names, record))

        record.name = "Foo.BarBaz"
        self.assertFalse(module.is_from_wanted_logger(wantted_names, record))

        record.name = "Qax"
        self.assertFalse(module.is_from_wanted_logger(wantted_names, record))
