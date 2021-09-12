import datetime
import logging
import re
import time
import unittest
import unittest.mock

import livy.client
import livy.logreader as module


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
        with self.assertRaises(TypeError):
            module.LivyBatchLogReader(self.client, 1234, timezone=8)

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
        with self.assertLogs("livy.logreader", "ERROR"):
            self.reader.read()

    def test_read_until_finish_block(self):
        self.client.get_batch_state.side_effect = ["running", "finished"]
        self.client.get_batch_log.return_value = []
        self.reader.read_until_finish(block=True, interval=0.01)

    def test_read_until_finish_unblock(self):
        self.client.get_batch_state.side_effect = ["running", "finished"]
        self.client.get_batch_log.return_value = []

        self.reader.read_until_finish(block=False, interval=0.5)

        # stop event
        self.reader._stop_event.set()

        # test error
        with self.assertRaises(Exception):
            self.reader.read_until_finish()

        # wait
        self.reader.thread.join()

    def test_stop_read_success(self):
        self.client.get_batch_state.return_value = "running"
        self.client.get_batch_log.return_value = []
        self.reader.read_until_finish(block=False, interval=1.0)
        tic = time.time()

        self.reader.stop_read()
        self.reader.thread.join()
        toc = time.time()

        self.assertLess(toc - tic, 1.0)

    def test_stop_read_fail(self):
        with self.assertRaises(Exception):
            self.reader.stop_read()


class ParserTester(unittest.TestCase):
    def test_default_parser(self):
        pattern, _ = module._BUILTIN_PARSERS["Default"]
        m = pattern.match("21/05/01 12:34:56 DEBUG Foo: test message")
        p = module.default_parser(m)

        assert isinstance(p, module.LivyLogParseResult)
        assert p.created == datetime.datetime(2021, 5, 1, 12, 34, 56)
        assert p.level == logging.DEBUG
        assert p.name == "Foo"
        assert p.message == "test message"

    def test_yarn_warning_parser(self):
        pattern, _ = module._BUILTIN_PARSERS["YARN warning"]
        m = pattern.match(
            "[Tue May 25 08:40:24 +0800 2021] Application is added to the scheduler and is not yet activated. Queue's AM resource limit exceeded.  Details : AM Partition = CORE; AM Resource Request = <memory:896, max memory:253952, vCores:1, max vCores:48>; Queue Resource Limit for AM = <memory:0, vCores:0>; User AM Resource Limit of the queue = <memory:0, vCores:0>; Queue AM Resource Usage = <memory:896, vCores:1>;"
        )
        p = module.yarn_warning_parser(m)

        assert isinstance(p, module.LivyLogParseResult)
        assert p.created.timestamp() == 1621903224
        assert p.message.startswith(
            "Application is added to the scheduler and is not yet activated."
        )

    def test_python_traceback_parser(self):
        pattern, _ = module._BUILTIN_PARSERS["Python Traceback"]
        m = pattern.match(
            'Traceback (most recent call last):\n  File "<string>", line 1, in <module>\nValueError'
        )
        p = module.python_traceback_parser(m)

        assert isinstance(p, module.LivyLogParseResult)
        assert p.created == None
        assert p.level == logging.ERROR
        assert p.message.startswith("\n  File")

    def test_python_warning_parser(self):
        pattern, _ = module._BUILTIN_PARSERS["Python warning"]
        m = pattern.match(
            '/livy/logreader.py:115: UserWarning: Test\n  warnings.warn("Test")'
        )
        p = module.python_warning_parser(m)

        assert isinstance(p, module.LivyLogParseResult)
        assert p.created == None
        assert p.level == logging.WARNING
        assert p.message.startswith("Test")
