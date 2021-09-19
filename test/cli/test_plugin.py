import argparse
import json
import os
import tempfile
import unittest
import unittest.mock

import livy.cli.plugin as module


class TestHelper(unittest.TestCase):
    def setUp(self) -> None:
        _, path = tempfile.mkstemp()
        self.config_path = path

        patcher = unittest.mock.patch("livy.cli.config.USER_CONFIG_PATH", path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        os.remove(self.config_path)

    def test_read_plugin_config_success(self):
        with open(self.config_path, "w") as fp:
            json.dump({"plugin:foo": {"foo": "bar"}}, fp)
        self.assertDictEqual(module.read_plugin_config("foo"), {"foo": "bar"})

    def test_read_plugin_config_file_not_exists(self):
        self.assertIsNone(module.read_plugin_config("foo"))

    def test_read_plugin_config_json_error(self):
        with open(self.config_path, "w") as fp:
            fp.write("{")
        self.assertIsNone(module.read_plugin_config("foo"))

    def test_read_plugin_config_section_not_exists(self):
        with open(self.config_path, "w") as fp:
            json.dump({"plugin:foo": {"foo": "bar"}}, fp)
        self.assertIsNone(module.read_plugin_config("foobar2"))


class TestUploadS3(unittest.TestCase):
    def setUp(self) -> None:
        # boto3
        self.client = unittest.mock.MagicMock()
        patcher = unittest.mock.patch("boto3.client", return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)

        # config
        patcher = unittest.mock.patch(
            "livy.cli.plugin.read_plugin_config",
            return_value={"bucket": "example-bucket", "folder_format": "/{uuid}/"},
        )
        self.read_config = patcher.start()
        self.addCleanup(patcher.stop)

        # args
        self.args = argparse.Namespace()
        self.args.script = "test.py"
        self.args.jars = []
        self.args.py_files = []
        self.args.files = []
        self.args.archives = []

    def test_success(self):
        self.args.script = "test.py"
        self.args.jars = ["lib.jar"]
        self.args.py_files = ["s3://another-bucket/lib.zip"]
        self.args.files = ["note.txt"]
        self.args.archives = ["archvie.tar.gz"]

        with unittest.mock.patch("uuid.uuid4", return_value="mock-uuid") as fp:
            module.upload_s3("PRE-SUBMIT", self.args)

        self.assertEqual(self.args.script, "s3://example-bucket/mock-uuid/test.py")
        self.assertSequenceEqual(
            self.args.jars, ["s3://example-bucket/mock-uuid/jars/lib.jar"]
        )
        self.assertSequenceEqual(self.args.py_files, ["s3://another-bucket/lib.zip"])
        self.assertSequenceEqual(
            self.args.files, ["s3://example-bucket/mock-uuid/files/note.txt"]
        )
        self.assertSequenceEqual(
            self.args.archives,
            ["s3://example-bucket/mock-uuid/archives/archvie.tar.gz"],
        )

    def test_success_with_expire(self):
        self.read_config.return_value = {
            "bucket": "example-bucket",
            "folder_format": "{uuid}",
            "expire_days": 5,
        }

        module.upload_s3("PRE-SUBMIT", self.args)

    def test_hook_reject(self):
        with self.assertRaises(Exception):
            module.upload_s3("NOT-THIS-HOOK", self.args)

    def test_expire_error(self):
        self.read_config.return_value = {
            "bucket": "example-bucket",
            "folder_format": "{uuid}",
            "expire_days": -1,
        }

        module.upload_s3("PRE-SUBMIT", self.args)
