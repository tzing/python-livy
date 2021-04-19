import livy.exception as exception


def test_TypeError():
    e = exception.TypeError("foo", "str", int)
    assert isinstance(str(e), str)

    e = exception.TypeError("foo", (float, str), 1234)
    assert isinstance(str(e), str)


def test_RequestError():
    e = exception.RequestError(123, "test")
    assert isinstance(str(e), str)

    e = exception.RequestError(123, "test", Exception())
    assert isinstance(str(e), str)
