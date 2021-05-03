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
        with self.assertRaises(TypeError):
            module.LivyBatchLogReader(self.client, 1234, prefix=object())

    def test_add_parsers(self):
        # success
        pattern = re.compile(r"(.+): (.+)")  # dummy
        self.reader.add_parsers(pattern, module.default_parser)

        # fail
        with self.assertRaises(TypeError):
            self.reader.add_parsers(r"(.+): (.+)", module.default_parser)
        with self.assertRaises(TypeError):
            self.reader.add_parsers(pattern, "1234")

    def test_read_success(self):
        self.client.get_batch_log.return_value = [
            "stdout: ",
            "test stdout extraction",
            "line 2",
            "21/05/01 15:21:03 INFO SecurityManager: Changing view acls to: livy",
            "21/05/01 15:21:03 INFO SecurityManager: Changing view acls to: livy",  # duplicated line
            "21/05/01 15:21:23 INFO Client: ",
            "\t client token: N/A",
            "\t diagnostics: AM container is launched, waiting for AM container to Register with RM",
            "\t ApplicationMaster host: N/A",
            "\t ApplicationMaster RPC port: -1",
            "\t queue: default",
            "\t start time: 1619882482318",
            "\t final status: UNDEFINED",
            "\t tracking URL: http://ip-10-104-21-141.us-west-2.compute.internal:20888/proxy/application_1618372323346_53932/",
            "\t user: livy",
            "extra stdout here",
            "\nstderr: ",
            "stderr log here",
            "\nYARN Diagnostics: ",
        ]

        with self.assertLogs("Client", "INFO"), self.assertLogs(
            "stdout", "INFO"
        ) as logS, self.assertLogs("stderr", "ERROR"):
            self.reader.read()

        self.assertEqual(len(logS.output), 2)

    def test_read_fail(self):
        pattern = re.compile("^ERROR:", re.MULTILINE)
        self.reader._parsers[pattern] = lambda: None  # signature not match

        self.client.get_batch_log.return_value = [
            "\nstderr: ",
            "ERROR: assert raise error on this line",
        ]
        with self.assertLogs("livy.log", "ERROR"):
            self.reader.read()


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
