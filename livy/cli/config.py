"""Configuration management for python-livy CLI tool."""
import argparse
import enum
import json
import logging
import typing

import livy.utils
import livy.utils.configbase


logger = logging.getLogger(__name__)


class RootSection(livy.utils.ConfigBase):
    """Prefix ``root``. Basic settings that might be applied to all actions."""

    api_url: str = None
    """Base-URL for Livy API server."""


class LocalLoggingSection(livy.utils.ConfigBase):
    """Prefix ``logs``. Logging behavior on local."""

    class LogLevel(enum.IntEnum):
        DEBUG = logging.DEBUG
        INFO = logging.INFO
        WARNING = logging.WARNING
        ERROR = logging.ERROR

    format: str = (
        "%(levelcolor)s%(asctime)s [%(levelname)s] %(name)s:%(reset)s %(message)s"
    )
    """Log message format. ``%(levelcolor)s`` and ``%(reset)s`` are two special
    keys only take effect on console output in this tool. They are and used to
    colorize the output. For all the other avaliable keys, see
    `log attributes <https://docs.python.org/3/library/logging.html#logrecord-attributes>`_."""

    date_format: str = "%Y-%m-%d %H:%M:%S %z"
    """Date format in log message. It uses
    `strftime <https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes>`_
    format."""

    with_progressbar: bool = True
    """Convert Spark TaskSetManager's logs (*Finished task X in stage Y on
    example.com (1/10)*) into progress bar. This feature requires
    `tqdm <https://tqdm.github.io/>`_."""

    highlight_loggers: typing.List[str] = []
    """Highlight logs from these loggers. This option only take effects when
    `colorama <https://pypi.org/project/colorama/>`_ is installed."""

    hide_loggers: typing.List[str] = []
    """Hide logs from these loggers. This option does not affect progress bar
    displaying."""

    output_file: bool = False
    """Output logs into file by default. A log file with random name would be
    created on the working directory when it set to ``True``."""

    logfile_level: LogLevel = logging.DEBUG
    """Default log level on output to log file."""


class ReadLogSection(livy.utils.ConfigBase):
    """Prefix ``read-log``. For :ref:`cli-read-log` tool."""

    keep_watch: bool = True
    """Keep watching for batch activity until it is finished."""


class SubmitSection(livy.utils.ConfigBase):
    """Prefix ``submit``. For :ref:`cli-submit` tool."""

    pre_submit: typing.List[str] = []
    """Enabled Pre-submit plugin list."""

    driver_memory: str = None
    """Amount of memory to use for the driver process. Need to specific unit,
    e.g. ``12gb`` or ``34mb``."""

    driver_cores: int = None
    """Number of cores to use for the driver process."""

    executor_memory: str = None
    """Amount of memory to use per executor process. Need to specific unit,
    e.g. ``12gb`` or ``34mb``."""

    executor_cores: int = None
    """Number of cores to use for each executor."""

    num_executors: int = None
    """Number of executors to launch for this batch."""

    spark_conf: typing.List[typing.Tuple[str, str]] = []
    """Key value pairs to override spark configuration properties."""

    watch_log: bool = True
    """Watching for logs after the task is submitted. This option shares the
    same behavior to :py:attr:`~ReadLogSection.keep_watch`, only different is the
    scope it take effects."""

    task_success: typing.List[str] = []
    """Plugins to be trigger when task is finished and success."""

    task_fail: typing.List[str] = []
    """Plugins to be trigger when task is ended and failed."""

    task_ended: typing.List[str] = []
    """Plugins to be trigger when task is ended, regardless to its state."""


class Configuration(livy.utils.ConfigBase):
    """Collection to all configurations"""

    root: RootSection
    logs: LocalLoggingSection
    read_log: ReadLogSection
    submit: SubmitSection


_configuration = None


def load() -> Configuration:
    """Load config"""
    # cache
    global _configuration
    if _configuration:
        return _configuration

    # read configs
    sections = {}
    for name, class_ in Configuration.__annotations__.items():
        sections[name] = class_.load(name)

    _configuration = Configuration(**sections)

    return _configuration


def main(argv=None):
    """CLI entrypoint"""
    # parse args
    parser = argparse.ArgumentParser(
        prog="livy config",
        description="%s All configured would be saved and loaded from %s"
        % (__doc__, livy.utils.configbase.USER_CONFIG_PATH),
    )
    action = parser.add_subparsers(title="action", dest="action")

    p = action.add_parser("list", help="List configurations")
    p.add_argument("section", nargs="?", help="Only show specific section")

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
        return cli_set_configure(args.name, args.value)

    elif args.action == "list":
        return cli_list_configure(args.section)

    else:
        print("Action is required: list/get/set")
        return 1


def cli_get_configure(name: str):
    """Get config, print on console"""
    if not check_conf_name_format(name):
        return 1

    section, key = name.lower().split(".", 1)

    cfg_root = load()
    cfg_group = getattr(cfg_root, section)
    value = getattr(cfg_group, key)

    print(f"{section}.{key} = {value}")

    return 0


def cli_set_configure(name: str, raw_input: str):
    """Set config"""
    if not check_conf_name_format(name):
        return 1

    # get section
    section, key = name.lower().split(".", 1)
    cfg_root = load()
    cfg_group = getattr(cfg_root, section)
    value_orig = getattr(cfg_group, key)
    dtype = cfg_group.__annotations__[key]

    # convert value
    try:
        value_new = convert_user_input(raw_input, dtype)
    except Exception as e:
        logger.error(
            "Failed to parse input `%s`. Extra message: %s",
            raw_input,
            e.args[0] if e.args else "none",
        )
        return 1

    is_changed = value_new != value_orig

    # show value
    print(f"{section}.{key} = {value_new}", end=" ")
    print("(updated)" if is_changed else "(not changed)")

    if not is_changed:
        return 0

    # write config file, read current file and override by current settings to
    # preserved extra fields used by config
    try:
        with open(livy.utils.configbase.USER_CONFIG_PATH, "r") as fp:
            config: dict = json.load(fp)
    except (FileNotFoundError, json.JSONDecodeError):
        config = {}

    # override with new settings; only add the changed value to the config file
    config.setdefault(section, {})[key] = value_new

    # save file
    try:
        with open(livy.utils.configbase.USER_CONFIG_PATH, "w") as fp:
            json.dump(config, fp, indent=2)
    except:
        logger.exception("Failed to write configure file")
        return 1

    return 0


def cli_list_configure(name: str):
    print("Current configuration keys:")

    if name:
        if not check_section_exist(name):
            return False

    for sec_name, sec_cls in Configuration.__annotations__.items():
        if name and name != sec_name:
            continue

        # section name and description
        doc: str = sec_cls.__doc__
        if doc:  # un-capitalize
            doc = doc[0].lower() + doc[1:]
        print(f"\n$ Section {sec_name}, {doc}:")

        # members
        # python can't attach __doc__ to variable, so the docsting could only
        # be read in sphinx
        for attr_name in sec_cls.__annotations__:
            print(f"  - {sec_name}.{attr_name}")

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
    if name.lower() in Configuration.__annotations__:
        return True
    logger.error("Given section name is invalid: %s", name)
    logger.error(
        "Acceptable section names: %s", ", ".join(Configuration.__annotations__)
    )
    return False


def check_key_exist(section: str, key: str) -> bool:
    """Check if key exists. It assumes section name is verified"""
    section_class = Configuration.__annotations__[section.lower()]
    if key.lower() in section_class.__annotations__:
        return True
    logger.error("Key `%s` does not exist in section %s", key, section)
    logger.error("Acceptable key names: %s", ", ".join(section_class.__annotations__))
    return False


def convert_user_input(s: str, dtype: type):
    """Convert user input str (from CLI) to related data type"""
    assert isinstance(s, str)
    if dtype is str:
        return s
    elif dtype is int:
        return int(s)
    elif dtype is bool:
        return convert_bool(s)
    elif isinstance(dtype, type) and issubclass(dtype, enum.Enum):
        return convert_enum(s, dtype)
    elif getattr(dtype, "__origin__", None) in (list, typing.List):
        # t = typing.List[str]
        # t.__origin__ = list        => Python >= 3.7
        #              = typing.List => Python 3.6
        return [convert_user_input(v, dtype.__args__[0]) for v in s.split(",")]

    assert False, f"data of type {dtype} is currently unsupported"


def convert_bool(s: str) -> bool:
    if isinstance(s, bool):
        return s
    s = str(s).lower()
    if s in ("1", "t", "true", "y", "yes"):
        return True
    elif s in ("0", "f", "false", "n", "no"):
        return False
    else:
        assert False, "should be [t]rue or [f]alse"


def convert_enum(s: str, enum_: enum.Enum) -> int:
    for e in enum_:
        if s == e.name:
            return e.value
    assert False, "unknown input %s. expected value= %s" % (
        s,
        " / ".join(e.name for e in enum_),
    )


if __name__ == "__main__":
    exit(main())
