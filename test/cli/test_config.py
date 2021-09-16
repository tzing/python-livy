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
