import json
import os
import tempfile
import unittest
import unittest.mock

import livy.cli.config as module


class TestConfig(unittest.TestCase):
    def setUp(self) -> None:
        _, path = tempfile.mkstemp()
        self.config_path = path

        patcher = unittest.mock.patch("livy.cli.config.MAIN_CONFIG_PATH", path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        module._settings = None
        os.remove(self.config_path)

    def test_load_default(self):
        s1 = module.load()
        s2 = module.load()
        assert s1 is s2

    def test_load_with_config(self):
        # prepare
        with open(self.config_path, "w") as fp:
            json.dump(
                {
                    "root": {
                        "api_url": "http://example.com/",
                    },
                    "read_log": {
                        "keep_watch": False,
                    },
                },
                fp,
            )

        # test
        s = module.load(self.config_path)

        assert s.root.api_url == "http://example.com/"
        assert s.read_log.keep_watch == False

    def test_main(self):
        self.assertEqual(0, module.main(["get", "root.api_url"]))
        self.assertEqual(0, module.main(["set", "root.api_url", "http://example.com/"]))
        self.assertEqual(0, module.main(["get", "root.api_url"]))
        self.assertEqual(0, module.main(["set", "root.api_url", "http://example.com/"]))

    def test_main_error(self):
        self.assertNotEqual(0, module.main(["get", "foo"]))
        self.assertNotEqual(0, module.main(["get", "foo.bar"]))
        self.assertNotEqual(0, module.main(["get", "root.foo"]))
        self.assertNotEqual(0, module.main(["set", "root.api_url", " "]))
        self.assertNotEqual(0, module.main(["set", "read_log.keep_watch", "foo"]))


class TestValidator(unittest.TestCase):
    def test_cbool(self):
        # true
        self.assertEqual(module.cbool(True), True)
        self.assertEqual(module.cbool("True"), True)
        self.assertEqual(module.cbool("yes"), True)
        self.assertEqual(module.cbool(1), True)

        # false
        self.assertEqual(module.cbool(False), False)
        self.assertEqual(module.cbool("F"), False)
        self.assertEqual(module.cbool("N"), False)
        self.assertEqual(module.cbool("0"), False)

        # fail
        with self.assertRaises(ValueError):
            module.cbool("foo")
