import unittest
import unittest.mock

import livy.cli.kill as module
import livy


class TestMain(unittest.TestCase):
    def setUp(self) -> None:
        # client
        self.client = unittest.mock.MagicMock(spec=livy.LivyClient)
        patcher = unittest.mock.patch("livy.LivyClient", return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.client.get_batch_information.return_value = {
            "id": 1234,
            "state": "running",
        }

        # user confirm prompt
        patcher = unittest.mock.patch("livy.cli.kill.check_user_confirm")
        self.check_user_confirm = patcher.start()
        self.addCleanup(patcher.stop)

    def test_success(self):
        self.client.is_batch_finished.side_effect = [False, True]
        self.assertEqual(0, module.main(["1234"]))

    def test_get_batch_information_errors(self):
        self.client.get_batch_information.side_effect = livy.RequestError(0, "test")
        self.assertEqual(1, module.main(["1234"]))

        self.client.get_batch_information.side_effect = KeyboardInterrupt()
        self.assertEqual(1, module.main(["1234"]))

    def test_task_finished(self):
        self.client.get_batch_information.return_value = {
            "id": 1234,
            "state": "failed",
        }
        self.assertEqual(1, module.main(["1234"]))

    def test_user_cancal(self):
        self.check_user_confirm.return_value = False
        self.assertEqual(1, module.main(["1234"]))

    def test_delete_batch_error(self):
        self.client.delete_batch.side_effect = livy.RequestError(0, "test")
        self.assertEqual(1, module.main(["1234"]))

        self.client.delete_batch.side_effect = KeyboardInterrupt()
        self.assertEqual(1, module.main(["1234"]))

    def test_monitor_error(self):
        self.client.is_batch_finished.side_effect = livy.RequestError(0, "test")
        self.assertEqual(1, module.main(["1234"]))

        self.client.is_batch_finished.side_effect = KeyboardInterrupt()
        self.assertEqual(1, module.main(["1234"]))


class TestCheckUserConfirm(unittest.TestCase):
    def setUp(self) -> None:
        patcher = unittest.mock.patch("builtins.input")
        self.input = patcher.start()
        self.addCleanup(patcher.stop)

    def test_success(self):
        self.input.return_value = "yes"
        self.assertTrue(module.check_user_confirm(1234))

        self.input.return_value = "no"
        self.assertFalse(module.check_user_confirm(1234))

    def test_no_input(self):
        self.input.return_value = " "
        self.assertFalse(module.check_user_confirm(1234))

    def test_invalid_input(self):
        self.input.return_value = "test"
        self.assertFalse(module.check_user_confirm(1234))

    def test_no_colorama(self):
        with unittest.mock.patch("importlib.import_module", side_effect=ImportError()):
            self.input.return_value = "yes"
            self.assertTrue(module.check_user_confirm(1234))
