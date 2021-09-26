import json
import os
import tempfile
import unittest
import unittest.mock

import livy.utils.config as module


class ConfigBaseTester(unittest.TestCase):
    def test___init__(self):
        class Foo(module.ConfigBase):
            baz: int = 456
            qax: int

        class Bar(module.ConfigBase):
            foo: Foo
            bar: int = 123
            qaz: str = "hello"

        t = Bar(qaz="world")

        self.assertEqual(t.bar, 123)
        self.assertEqual(t.qaz, "world")
        self.assertEqual(t.foo.baz, 456)
        self.assertIsNone(t.foo.qax)

    def test___repr__(self):
        class Foo(module.ConfigBase):
            bar: int = 123
            qaz: str = "hello"

        t = Foo()
        assert repr(t) == "Foo(bar=123, qaz=hello)"

    def test_mergedict(self):
        class Foo(module.ConfigBase):
            baz: int
            qax: int

        a = Foo(baz=1, qax=2)
        a.mergedict({"qax": 3, "not-related": 5})
        assert a.baz == 1
        assert a.qax == 3

    def test_load(self):
        # setup
        _, file_ok1 = tempfile.mkstemp()
        _, file_ok2 = tempfile.mkstemp()
        _, file_error = tempfile.mkstemp()
        self.addCleanup(lambda: os.remove(file_ok1))
        self.addCleanup(lambda: os.remove(file_ok2))
        self.addCleanup(lambda: os.remove(file_error))

        patcher = unittest.mock.patch(
            "livy.utils.config.CONFIG_LOAD_ORDER",
            [file_ok1, file_ok2, file_error, "/file-not-exist"],
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        with open(file_ok1, "w") as fp:
            json.dump({"foo": {"bar": 1, "baz": 2}}, fp)
        with open(file_ok2, "w") as fp:
            json.dump({"foo": {"baz": 3, "qax": 4}}, fp)
        with open(file_error, "w") as fp:
            fp.write("{")

        class Foo(module.ConfigBase):
            bar: int
            baz: int
            qax: int

        # test: success
        cfg = Foo.load("foo")
        self.assertEqual(cfg.bar, 1)
        self.assertEqual(cfg.baz, 3)
        self.assertEqual(cfg.qax, 4)

        # test: ignored
        cfg = Foo.load("invalid")
        self.assertIsNone(cfg.bar, None)
