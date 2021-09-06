import unittest
import unittest.mock


class TestMain(unittest.TestCase):
    def setUp(self) -> None:
        patcher = unittest.mock.patch(
            "livy.__main__._ENTRYPOINT",
            {
                "config": lambda x: 0,
            },
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_cli(self, *argv):
        patcher = unittest.mock.patch("sys.argv", argv)
        patcher.start()
        self.addCleanup(patcher.stop)

        import livy.__main__ as module

        return module.main()

    def test_success(self):
        self.assertEqual(0, self.run_cli("livy", "config", "foo"))

    def test_missing_subcmd(self):
        self.assertNotEqual(0, self.run_cli("livy"))

    def test_invalid_subcmd(self):
        with self.assertRaises(SystemExit):
            self.assertNotEqual(0, self.run_cli("livy", "foo"))

    def test_help(self):
        # `-h` should bypass to subcommand
        self.assertEqual(0, self.run_cli("livy", "config", "-h"))

        # `-h` is consumed by this command
        with self.assertRaises(SystemExit):
            self.assertNotEqual(0, self.run_cli("livy", "-h"))
