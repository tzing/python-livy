import builtins
import typing


class Error(Exception):
    """Base exception type for python-livy package"""


class TypeError(Error, builtins.TypeError):
    """Wrapped type error type, for easier printing more information"""

    def __init__(
        self,
        name: str,
        expect: typing.Union[str, type],
        got: typing.Union[str, type],
    ) -> None:
        self.name = name
        self.expect = expect
        self.got = got

    def __str__(self) -> str:
        expect = self.type_name(self.expect)
        got = self.type_name(self.got)
        return f"Expect {expect} for {self.name}, got {got}"

    @classmethod
    def type_name(cls, obj):
        if isinstance(obj, (list, tuple)):
            *pre, last = [cls.type_name(sub) for sub in obj]
            return ", ".join(pre) + " or " + last
        if isinstance(obj, str):
            return obj
        elif isinstance(obj, type):
            return obj.__name__
        else:
            return cls.type_name(type(obj))


class UnsupportedError(Error, NotImplementedError):
    """Request is unsupported"""


class RequestError(Error, IOError):
    """Error during data transportation"""

    def __init__(self, code: int, reason: str, error=None) -> None:
        self.code = code
        self.reason = reason
        self.error = error

    def __str__(self) -> str:
        msg = f"RequestError: Code{self.code} ({self.reason})"
        if self.error:
            msg += f". exception={self.error}"
        return msg
