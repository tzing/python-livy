import logging
import re
import unittest
import unittest.mock

import livy.client
import livy.log as module


class LivyBatchLogReaderTester(unittest.TestCase):
    def setUp(self) -> None:
        self.client = unittest.mock.MagicMock(spec=livy.client.LivyClient)
        self.reader = module.LivyBatchLogReader(self.client, 1234)

    def test___init__(self):
        # only test fail
        with self.assertRaises(TypeError):
            module.LivyBatchLogReader(object(), 1234)
        with self.assertRaises(TypeError):
            module.LivyBatchLogReader(self.client, "1234")

    def test_add_parsers(self):
        # success
        pattern = re.compile(r"(.+): (.+)")  # dummy
        self.reader.add_parsers(pattern, module.default_parser)

        # fail
        with self.assertRaises(TypeError):
            self.reader.add_parsers(r"(.+): (.+)", module.default_parser)
        with self.assertRaises(TypeError):
            self.reader.add_parsers(pattern, "1234")


class ParserTester(unittest.TestCase):
    def test_default_parser(self):
        pattern = re.compile(
            r"^(\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}) ([A-Z]+) (.+?):(.*(?:\n\t.+)*)"
        )

        m = pattern.match("21/05/01 12:34:56 DEBUG Foo: test message")
        p = module.default_parser(m)

        assert isinstance(p, module.LivyLogParseResult)
        assert p.created == 1619843696
        assert p.level == logging.DEBUG
        assert p.name == "Foo"
        assert p.message == "test message"

    def test_convert_stdout(self):
        p = module.convert_stdout("Foo")

        assert isinstance(p, module.LivyLogParseResult)
        assert p.message == "Foo"
