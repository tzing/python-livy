import enum
import json
import os
import tempfile
import typing
import unittest
import unittest.mock

import livy.cli.config as module


class TestMain(unittest.TestCase):
    def test_main(self):
        with unittest.mock.patch("livy.cli.config.cli_get_configure", return_value=0):
            self.assertEqual(0, module.main(["get", "foo.bar"]))

        with unittest.mock.patch("livy.cli.config.cli_set_configure", return_value=0):
            self.assertEqual(0, module.main(["set", "foo.bar", "test"]))

        with unittest.mock.patch("livy.cli.config.cli_list_configure", return_value=0):
            self.assertEqual(0, module.main(["list"]))

        self.assertEqual(1, module.main([]))

    def test_cli_get_configure(self):
        self.assertEqual(0, module.cli_get_configure("root.api_url"))

        # also test assisting functions here
        self.assertEqual(1, module.cli_get_configure(""))
        self.assertEqual(1, module.cli_get_configure("foo"))
        self.assertEqual(1, module.cli_get_configure("foo.bar"))
        self.assertEqual(1, module.cli_get_configure("root.foo"))

    def test_cli_set_configure(self):
        _, path = tempfile.mkstemp()
        self.addCleanup(lambda: os.remove(path))

        # success
        with unittest.mock.patch(
            "livy.cli.config.USER_CONFIG_PATH", path
        ), unittest.mock.patch(
            "livy.cli.config.convert_user_input", return_value="foo"
        ):
            self.assertEqual(0, module.cli_set_configure("root.api_url", "test"))

            # for early escape
            self.assertEqual(0, module.cli_set_configure("root.api_url", "test"))

    def test_cli_set_configure_error(self):
        # section not exist
        self.assertEqual(1, module.cli_set_configure("foo", "bar"))

        # parse error
        with unittest.mock.patch(
            "livy.cli.config.convert_user_input", side_effect=Exception()
        ):
            self.assertEqual(1, module.cli_set_configure("root.api_url", "test"))

        # file open error
        with unittest.mock.patch(
            "livy.cli.config.open", side_effect=FileNotFoundError()
        ), unittest.mock.patch(
            "livy.cli.config.convert_user_input", return_value="bar"
        ):
            self.assertEqual(1, module.cli_set_configure("root.api_url", "test"))

    def test_cli_list_configure(self):
        # list all
        self.assertEqual(0, module.cli_list_configure(""))

        # list only specific section
        self.assertEqual(0, module.cli_list_configure("root"))

        # list only specific section - but invalid
        self.assertEqual(0, module.cli_list_configure("no-this-section"))


class TestConvertUserInput(unittest.TestCase):
    class FooEnum(enum.Enum):
        FOO = 5
        BAR = 10

    def test_convert_user_input(self):
        self.assertEqual(module.convert_user_input("foo", str), "foo")
        self.assertEqual(module.convert_user_input("1234", int), 1234)

        with unittest.mock.patch("livy.cli.config.convert_bool", return_value=True):
            self.assertEqual(module.convert_user_input("t", bool), True)

        with unittest.mock.patch("livy.cli.config.convert_enum", return_value=10):

            self.assertEqual(module.convert_user_input("BAR", self.FooEnum), 10)

        self.assertSequenceEqual(
            module.convert_user_input("foo,bar", typing.List[str]), ["foo", "bar"]
        )

        with self.assertRaises(AssertionError):
            module.convert_user_input("foo", object)

    def test_convert_bool(self):
        # true
        self.assertEqual(module.convert_bool(True), True)
        self.assertEqual(module.convert_bool("True"), True)
        self.assertEqual(module.convert_bool("yes"), True)
        self.assertEqual(module.convert_bool(1), True)

        # false
        self.assertEqual(module.convert_bool(False), False)
        self.assertEqual(module.convert_bool("F"), False)
        self.assertEqual(module.convert_bool("N"), False)
        self.assertEqual(module.convert_bool("0"), False)

        # fail
        with self.assertRaises(AssertionError):
            module.convert_bool("foo")

    def test_convert_enum(self):
        # success
        self.assertEqual(module.convert_enum("FOO", self.FooEnum), 5)
        self.assertEqual(module.convert_enum("BAR", self.FooEnum), 10)

        # fail
        with self.assertRaises(AssertionError):
            module.convert_enum("test", self.FooEnum)
