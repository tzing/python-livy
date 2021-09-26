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
