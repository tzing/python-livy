import argparse
import dataclasses
import os
import pathlib
import json

MAIN_CONFIG_PATH = pathlib.Path.home() / ".config" / "python-livy-config.json"


@dataclasses.dataclass
class _RootSection:
    api_url: str = None


@dataclasses.dataclass
class _ReadLogSection:
    keep_watch: bool = True


@dataclasses.dataclass
class _Settings:
    root: _RootSection = dataclasses.field(default_factory=_RootSection)
    read_log: _ReadLogSection = dataclasses.field(default_factory=_ReadLogSection)


_settings = None


def load(path=MAIN_CONFIG_PATH) -> _Settings:
    """Load config"""
    # cache
    global _settings
    if _settings:
        return _settings

    # create default object if not exist
    if not os.path.isfile(path):
        _settings = _Settings()
        return _settings

    # read existing config
    with open(path, "rb") as fp:
        data = json.load(fp)

    def from_dict(cls, d: dict):
        obj = cls()
        for name, type_ in cls.__annotations__.items():
            if dataclasses.is_dataclass(type_):
                value = from_dict(type_, d.get(name, {}))
            else:
                value = d.get(name, None)
            setattr(obj, name, value)
        return obj

    _settings = from_dict(_Settings, data)
    return _settings
