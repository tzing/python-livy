import argparse
import datetime
import logging
import unittest
import unittest.mock

import livy.cli.submit as module
import livy.cli.config
import livy


class TestMain(unittest.TestCase):
    def setUp(self) -> None:
        # config getter
        self.config = livy.cli.config.Configuration()
        self.config.root.api_url = "http://example.com/"
        patcher = unittest.mock.patch("livy.cli.config.load", return_value=self.config)
        patcher.start()
        self.addCleanup(patcher.stop)

        # hook getter
        patcher = unittest.mock.patch(
            "livy.cli.submit.get_function", return_value=lambda x, y: y
        )
        self.get_function = patcher.start()
        self.addCleanup(patcher.stop)

        # livy client
        self.client = unittest.mock.MagicMock(spec=livy.LivyClient)
        self.client.create_batch.return_value = {"id": 1234}
        patcher = unittest.mock.patch("livy.LivyClient", return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)

        # log reader
        self.reader = unittest.mock.MagicMock(spec=livy.LivyBatchLogReader)
        patcher = unittest.mock.patch(
            "livy.LivyBatchLogReader", return_value=self.reader
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_success(self):
        # not reading log
        self.assertEqual(
            0,
            module.main(
                [
                    "test.py",
                    "--on-pre-submit",
                    "test_hook",
                    "--no-watch-log",
                ]
            ),
        )

        self.get_function.assert_called()
        self.client.check.assert_called()

        # reading log
        self.client.get_batch_state.return_value = "success"
        self.assertEqual(
            0,
            module.main(
                [
                    "test.py",
                    "--on-pre-submit",
                    "test_hook",
                ]
            ),
        )

    def test_server_error(self):
        self.client.check.side_effect = livy.RequestError(0, "Test error")
        self.assertEqual(1, module.main(["test.py"]))

    def test_create_batch_error_1(self):
        self.client.create_batch.side_effect = livy.RequestError(0, "Test error")
        self.assertEqual(1, module.main(["test.py"]))

    def test_create_batch_error_2(self):
        self.client.create_batch.return_value = {"foo": "bar"}
        self.assertEqual(1, module.main(["test.py"]))

    def test_readlog_error(self):
        self.reader.read_until_finish.side_effect = livy.RequestError(0, "Test error")
        self.assertEqual(1, module.main(["test.py"]))

    def test_readlog_interrupt(self):
        self.reader.read_until_finish.side_effect = KeyboardInterrupt()
        self.assertEqual(1, module.main(["test.py"]))

    def test_ending_get_batch_state(self):
        self.client.get_batch_state.side_effect = livy.RequestError(0, "Test error")
        self.assertEqual(1, module.main(["test.py"]))

    def test_task_ending_error(self):
        self.client.get_batch_state.return_value = "dead"
        self.assertEqual(1, module.main(["test.py"]))


class TestHelperFunc(unittest.TestCase):
    def test_argument(self):
        # memory size
        self.assertEqual(module.argmem("1234GB"), "1234GB")
        with self.assertRaises(argparse.ArgumentTypeError):
            module.argmem("1234")

        # key value pair
        self.assertEqual(module.argkvpair("foo=bar=baz"), ("foo", "bar=baz"))
        with self.assertRaises(ValueError):
            module.argkvpair("1234")

    def test_get_function(self):
        # success
        func = module.get_function("livy.cli.submit:get_function")
        self.assertIs(func, module.get_function)

        # nameing format error
        self.assertIsNone(module.get_function("::error"))

        # import error
        self.assertIsNone(module.get_function("no_this_module:example"))

        # func error
        self.assertIsNone(module.get_function("livy.cli.submit:no_this_func"))

    def test_run_hook(self):
        logger = logging.getLogger(__name__)
        args = unittest.mock.Mock(spec=argparse.Namespace)

        patcher = unittest.mock.patch(
            "livy.cli.submit.get_function", return_value=lambda x, y: y
        )

        # success
        with patcher:
            module.run_hook(logger, "TEST", args, ["foo"])

        # failed to get func
        with patcher as patch, self.assertRaises(SystemExit):
            patch.return_value = None
            module.run_hook(logger, "TEST", args, ["foo"])

        # error during run action
        with patcher as patch, self.assertRaises(SystemExit):
            patch.return_value = unittest.mock.Mock(side_effect=ValueError())
            module.run_hook(logger, "TEST", args, ["foo"])

        # hook action's return value invalid
        with patcher as patch, self.assertRaises(SystemExit):
            patch.return_value = lambda x, y: None
            module.run_hook(logger, "TEST", args, ["foo"])

    def test_human_readable_timeperiod(self):
        self.assertEqual(
            "1h 5s",
            module.human_readable_timeperiod(datetime.timedelta(seconds=3605)),
        )
        self.assertEqual(
            "2d 1m 1s",
            module.human_readable_timeperiod(datetime.timedelta(days=2, seconds=61)),
        )
