import unittest
import unittest.mock

import livy.cli.read_log as module
import livy


class TestMain(unittest.TestCase):
    def setUp(self) -> None:
        self.client = unittest.mock.MagicMock(spec=livy.LivyClient)
        self.reader = unittest.mock.Mock(spec=livy.LivyBatchLogReader)

        patcher = unittest.mock.patch("livy.LivyClient", return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)

        patcher = unittest.mock.patch(
            "livy.LivyBatchLogReader", return_value=self.reader
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_success(self):
        self.client.is_batch_ended.return_value = False
        module.main(["--api-url", "http://example.com", "--keep-watch", "1234"])

    def test_server_error(self):
        self.client.is_batch_ended.side_effect = livy.RequestError(0, "foo")
        module.main(["--api-url", "http://example.com", "--keep-watch", "1234"])

    def test_read_once(self):
        module.main(["--api-url", "http://example.com", "--no-keep-watch", "1234"])

    def test_read_error(self):
        self.reader.read.side_effect = livy.RequestError(0, "foo")
        module.main(["--api-url", "http://example.com", "--no-keep-watch", "1234"])

    def test_keyboard_interrupt(self):
        # on reading log
        self.reader.read.side_effect = KeyboardInterrupt()
        module.main(["--api-url", "http://example.com", "--no-keep-watch", "1234"])

        # on initial check
        self.client.is_batch_ended.side_effect = KeyboardInterrupt()
        module.main(["--api-url", "http://example.com", "--no-keep-watch", "1234"])
