"""Configuration management for python-livy CLI tool."""
import argparse
import dataclasses
import json
import logging
import pathlib
import re
import typing


MAIN_CONFIG_PATH = pathlib.Path.home() / ".config" / "python-livy.json"

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class _RootSection:
    """Basic settings that might be applied to all actions."""

    api_url: str = None
    """Base-URL for Livy API server"""


T_LOGLEVEL = typing.TypeVar("T_LOGLEVEL")


@dataclasses.dataclass
class _LocalLogSection:
    """Configure logging behavior on local"""

    format: str = (
        "%(levelcolor)s%(asctime)s [%(levelname)s] %(name)s:%(reset)s %(message)s"
    )
    """Log message format."""

    date_format: str = "%Y-%m-%d %H:%M:%S %z"
    """Date format in log message"""

    output_file: bool = False
    """Output logs into file by default"""

    logfile_level: T_LOGLEVEL = "DEBUG"
    """Default log level on output to log file"""

    with_progressbar: bool = True
    """Convert TaskSetManager's logs into progress bar"""


@dataclasses.dataclass
class _ReadLogSection:
    keep_watch: bool = True
    """Keep watching for batch activity until it is finished."""


@dataclasses.dataclass
class _Settings:
    root: _RootSection = dataclasses.field(default_factory=_RootSection)
    logs: _LocalLogSection = dataclasses.field(default_factory=_LocalLogSection)
    read_log: _ReadLogSection = dataclasses.field(default_factory=_ReadLogSection)


_settings = None


def load(path=None) -> _Settings:
    """Load config"""
    # cache
    global _settings
    if _settings:
        return _settings

    if not path:  # fill with default later, for easier testing
        path = MAIN_CONFIG_PATH

    # read existing config
    try:
        with open(path, "rb") as fp:
            data = json.load(fp)
    except (FileNotFoundError, json.JSONDecodeError):
        _settings = _Settings()
        return _settings

    def from_dict(cls, d: dict):
        obj = cls()
        for name, type_ in cls.__annotations__.items():
            if dataclasses.is_dataclass(type_):
                value = from_dict(type_, d.get(name, {}))
            else:
                value = d.get(name, getattr(obj, name))
            setattr(obj, name, value)
        return obj

    _settings = from_dict(_Settings, data)
    return _settings


def cbool(s: str) -> bool:
    if isinstance(s, bool):
        return s
    s = str(s).lower()
    if s in ("1", "t", "true", "y", "yes"):
        return True
    elif s in ("0", "f", "false", "n", "no"):
        return False
    else:
        raise ValueError()


def main(argv=None):
    """CLI entrypoint"""
    # parse args
    parser = argparse.ArgumentParser(
        prog="livy-config",
        description=f"{__doc__} All configured would be saved and loaded in {MAIN_CONFIG_PATH}.",
    )
    action = parser.add_subparsers(title="action", dest="action")

    p = action.add_parser("list", help="List configurations")

    p = action.add_parser("get", help="Get config value")
    p.add_argument("name", help="Config name to be retrieved.")

    p = action.add_parser("set", help="Set config value")
    p.add_argument("name", help="Name of config to be updated.")
    p.add_argument("value", help="Value to be set.")

    args = parser.parse_args(argv)

    # run
    if args.action == "get":
        return cli_get_configure(args.name)

    elif args.action == "set":
        raise NotImplementedError()

    elif args.action == "list":
        raise NotImplementedError()

    else:
        print("Action is required: list/get/set")
        return 1

    # # otherwise, action: set
    # value_given = args.value
    # if not value_given.strip():
    #     console.error("Could not set value as none")
    #     return 1

    # dtype = section.__annotations__[key_name]
    # try:
    #     if dtype is str:
    #         ...
    #     elif dtype is bool:
    #         value_given = cbool(value_given)
    #     elif dtype is T_LOGLEVEL:
    #         assert value_given in ("DEBUG", "INFO", "WARNING", "ERROR")
    #     else:
    #         logger.warning("Unregistered data type %s", dtype)  # pragma: no cover
    # except:
    #     logger.error("Failed to parse given input %s into %s type", value_given, dtype)
    #     return 1

    # if value_given == value_original:
    #     console.info("%s.%s = %s (not changed)", section_name, key_name, value_given)
    #     return 0

    # setattr(section, key_name, value_given)

    # # write value file
    # logger.debug("Write config to %s", MAIN_CONFIG_PATH)

    # with open(MAIN_CONFIG_PATH, "w") as fp:
    #     json.dump(dataclasses.asdict(config), fp, indent=2)

    # console.info("%s.%s = %s (updated)", section_name, key_name, value_given)
    # return 0


def cli_get_configure(name: str):
    """Get configure, print on console"""
    if not check_conf_name_format(name):
        return 1

    section, key = name.split(".", 1)

    cfg_root = load()
    cfg_group = getattr(cfg_root, section.lower())
    value = getattr(cfg_group, key.lower())

    print(f"{name} = {value}")

    return 0


def check_conf_name_format(name: str) -> bool:
    """Check if key format is right"""
    if not name.strip():
        logger.error("Empty name is given")
        return False

    # no separator
    cnt_dots = name.count(".")
    if cnt_dots != 1:
        logger.error("Config name is always in `section.key` format")
        if cnt_dots == 0:
            check_section_exist(name)
        return False

    # get and check keys
    section, key = name.split(".", 1)

    if not check_section_exist(section):
        return False

    if not check_key_exist(section, key):
        return False

    return True


def check_section_exist(name: str) -> bool:
    """Check if section name exists"""
    if name.lower() in _Settings.__annotations__:
        return True
    logger.error("Given section name is invalid: %s", name)
    logger.error("Acceptable section names: %s", ", ".join(_Settings.__annotations__))
    return False


def check_key_exist(section: str, key: str) -> bool:
    """Check if key exists. It assumes section name is verified."""
    section_class = _Settings.__annotations__[section.lower()]
    if key.lower() in section_class.__annotations__:
        return True
    logger.error("Key `%s` does not exist in section %s", key, section)
    logger.error("Acceptable key names: %s", ", ".join(section_class.__annotations__))
    return False


if __name__ == "__main__":
    exit(main())
