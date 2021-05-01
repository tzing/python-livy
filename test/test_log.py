import logging
import re
import unittest.mock

import pytest

import livy.client
import livy.log as module


def test_default_parser():
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


def test_convert_stdout():
    p = module.convert_stdout("Foo")

    assert isinstance(p, module.LivyLogParseResult)
    assert p.message == "Foo"


@pytest.fixture
def client():
    return unittest.mock.MagicMock(spec=livy.client.LivyClient)


def test_LivyBatchLogReader_init(client):
    # success
    module.LivyBatchLogReader(client, 1234)

    # fail
    with pytest.raises(TypeError):
        module.LivyBatchLogReader(object(), 1234)
    with pytest.raises(TypeError):
        module.LivyBatchLogReader(client, "1234")
