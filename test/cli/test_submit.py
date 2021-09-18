import argparse
import unittest
import unittest.mock

import livy.cli.submit as module
import livy.cli.config
import livy


class TestMain(unittest.TestCase):
    def setUp(self) -> None:
        # config getter
        self.config = livy.cli.config.Configuration()
        patcher = unittest.mock.patch("livy.cli.config.load", return_value=self.config)
        patcher.start()
        self.addCleanup(patcher.stop)

        # hook getter
        patcher = unittest.mock.patch(
            "livy.cli.submit.get_function", return_value=lambda x: x
        )
        self.get_presubmit = patcher.start()
        self.addCleanup(patcher.stop)

        # livy client
        self.client = unittest.mock.MagicMock(spec=livy.LivyClient)
        patcher = unittest.mock.patch("livy.LivyClient", return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_success(self):
        self.client.create_batch.return_value = {"id": 1234}

        self.assertEqual(
            0,
            module.main(
                [
                    "test.py",
                    "--api-url",
                    "http://example.com/",
                    "--pre-submit",
                    "test_hook",
                ]
            ),
        )

        self.get_presubmit.assert_called()
        self.client.check.assert_called()

    def test_pre_submit_error(self):
        self.config.root.api_url = "http://example.com/"
        self.config.submit.pre_submit = ["test_hook"]

        # failed to get func
        self.get_presubmit.return_value = None
        self.assertEqual(1, module.main(["test.py"]))

        # error during run action
        self.get_presubmit.return_value = unittest.mock.Mock(side_effect=ValueError())
        self.assertEqual(1, module.main(["test.py"]))

        # hook action's return value invalid
        self.get_presubmit.return_value = lambda x: None
        self.assertEqual(1, module.main(["test.py"]))

    def test_server_error(self):
        self.config.root.api_url = "http://example.com/"
        self.client.check.side_effect = livy.RequestError(0, "Test error")

        self.assertEqual(1, module.main(["test.py"]))

    def test_create_batch_error(self):
        self.config.root.api_url = "http://example.com/"
        self.client.create_batch.side_effect = livy.RequestError(0, "Test error")

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
