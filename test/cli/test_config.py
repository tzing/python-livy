import enum
import json
import os
import tempfile
import unittest
import unittest.mock

import livy.cli.config as module


class TestConfigSectionBase(unittest.TestCase):
    def test___init__(self):
        class Foo(module.ConfigSectionBase):
            baz: int = 456
            qax: int

        class Bar(module.ConfigSectionBase):
            foo: Foo
            bar: int = 123
            qaz: str = "hello"

        t = Bar(qaz="world")

        assert t.bar == 123
        assert t.foo.baz == 456
        assert t.qaz == "world"

    def test___repr__(self):
        class Foo(module.ConfigSectionBase):
            bar: int = 123
            qaz: str = "hello"

        t = Foo()
        assert repr(t) == "Foo(bar=123, qaz=hello)"

    def test_merge(self):
        # data
        class Foo(module.ConfigSectionBase):
            baz: int
            qax: int

        a = Foo()
        a.baz = 1
        a.qax = 2

        b = Foo()
        b.qax = 3
        b.foo = 4
        assert a.baz == 1
        assert a.qax == 2

        # run
        a.merge(b)

        # check
        assert a.baz == 1
        assert a.qax == 3
        with self.assertRaises(AttributeError):
            a.foo

    def test_from_dict(self):
        class Foo(module.ConfigSectionBase):
            baz: int = 456

        class Bar(module.ConfigSectionBase):
            foo: Foo
            bar: int = 123

        # success
        t = Bar.from_dict({"foo": {"baz": 789}})
        assert isinstance(t, Bar)
        assert t.bar == 123
        assert t.foo.baz == 789

        # failed
        t = Bar.from_dict({"foo": 3})
        assert isinstance(t, Bar)
        assert t.foo.baz == 456

    def test_to_dict(self):
        class Foo(module.ConfigSectionBase):
            baz: int = 456
            qaz: str = "hello"

        class Bar(module.ConfigSectionBase):
            foo: Foo
            bar: int = 123

        self.assertDictEqual(
            Bar().to_dict(),
            {
                "bar": 123,
                "foo": {
                    "baz": 456,
                    "qaz": "hello",
                },
            },
        )


class TestConfiguration(unittest.TestCase):
    def setUp(self) -> None:
        _, path = tempfile.mkstemp()
        self.config_path = path

        patcher = unittest.mock.patch("livy.cli.config.CONFIG_LOAD_ORDER", [path])
        patcher.start()
        self.addCleanup(patcher.stop)

        patcher = unittest.mock.patch("livy.cli.config._configuration", None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        os.remove(self.config_path)

    def test_load_default(self):
        s1 = module.load()
        s2 = module.load()
        assert s1 is s2  # cache

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
        s = module.load()

        assert s.root.api_url == "http://example.com/"
        assert s.read_log.keep_watch == False


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
