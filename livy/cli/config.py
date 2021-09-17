"""Configuration management for python-livy CLI tool."""
import abc
import argparse
import enum
import json
import logging
import pathlib
import typing


USER_CONFIG_PATH = pathlib.Path.home() / ".config" / "python-livy.json"
CONFIG_LOAD_ORDER = [
    pathlib.Path(__file__).resolve().parent.parent / "default-configure.json",
    USER_CONFIG_PATH,
]


logger = logging.getLogger(__name__)


class ConfigSectionBase(abc.ABC):
    """Base class for configures, inspired by python3.7 dataclass."""

    __missing = object()

    def __init__(self, **kwargs) -> None:
        cls = type(self)
        for name, dtype in cls.__annotations__.items():
            # get value, or create one
            value = kwargs.get(name, self.__missing)
            if value is not self.__missing:
                # use user specific value
                ...
            elif isinstance(dtype, type) and issubclass(dtype, ConfigSectionBase):
                # auto initalized subsection class
                value = dtype()
            else:
                # get default value from class
                value = cls.__dict__.get(name, self.__missing)

            # set value if exists
            if value is self.__missing:
                continue
            self.__dict__[name] = value

    def __repr__(self) -> str:
        cls_name = type(self).__name__
        attr_values = []
        for k, v in self.__dict__.items():
            attr_values.append(f"{k}={v}")
        return f"{cls_name}({', '.join( attr_values)})"

    def merge(self, other: "ConfigSectionBase"):
        assert isinstance(other, type(self))
        for k in self.__dict__:
            v = other.__dict__.get(k, self.__missing)
            if v is self.__missing:
                continue
            self.__dict__[k] = v

    @classmethod
    def from_dict(cls, d: dict) -> "ConfigSectionBase":
        ins = cls()
        for name, dtype in cls.__annotations__.items():
            value = d.get(name, cls.__missing)
            if value is cls.__missing:
                continue
            if isinstance(dtype, type) and issubclass(dtype, ConfigSectionBase):
                if isinstance(value, dict):
                    value = dtype.from_dict(value)
                else:
                    logger.warning(
                        "Config parsing error. Expect dict for %s, got %s.",
                        name,
                        type(value).__name__,
                    )
                    continue
            # TODO validate
            ins.__dict__[name] = value
        return ins

    def to_dict(self):
        return {
            k: (v if not isinstance(v, ConfigSectionBase) else v.to_dict())
            for k, v in self.__dict__.items()
        }


class Configuration(ConfigSectionBase):
    """Collection to all configurations"""

    class RootSection(ConfigSectionBase):
        """Basic settings that might be applied to all actions"""

        api_url: str = None
        """Base-URL for Livy API server"""

    class LocalLoggingSection(ConfigSectionBase):
        """Logging behavior on local"""

        class LogLevel(enum.IntEnum):
            DEBUG = logging.DEBUG
            INFO = logging.INFO
            WARNING = logging.WARNING
            ERROR = logging.ERROR

        format: str = (
            "%(levelcolor)s%(asctime)s [%(levelname)s] %(name)s:%(reset)s %(message)s"
        )
        """Log message format."""

        date_format: str = "%Y-%m-%d %H:%M:%S %z"
        """Date format in log message"""

        output_file: bool = False
        """Output logs into file by default"""

        logfile_level: LogLevel = logging.DEBUG
        """Default log level on output to log file"""

        with_progressbar: bool = True
        """Convert TaskSetManager's logs into progress bar"""

    class ReadLogSection(ConfigSectionBase):
        """For read-log tool"""

        keep_watch: bool = True
        """Keep watching for batch activity until it is finished."""

    class SubmitSection(ConfigSectionBase):
        """For task submission tool"""

        pre_submit: typing.List[str] = None
        """Pre-submit processor list"""

        driver_memory: str = None
        """Amount of memory to use for the driver process."""

        driver_cores: int = None
        """Number of cores to use for the driver process"""

        executor_memory: str = None
        """Amount of memory to use per executor process"""

        executor_cores: int = None
        """Number of cores to use for each executor"""

        num_executors: int = None
        """Number of executors to launch for this batch"""

        spark_conf: typing.List[typing.Tuple[str, str]] = None
        """Spark configuration properties"""

        watch_log: bool = True
        """Watching for logs after the task is submitted"""

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

    _configuration = Configuration()

    # read configs
    for path in CONFIG_LOAD_ORDER:
        # read file
        try:
            with open(path, "rb") as fp:
                data = json.load(fp)
        except (FileNotFoundError, json.JSONDecodeError):
            continue

        # apply default values
        v = Configuration.from_dict(data)
        _configuration.merge(v)

    return _configuration


def main(argv=None):
    """CLI entrypoint"""
    # parse args
    parser = argparse.ArgumentParser(
        prog="livy-config",
        description=f"{__doc__} All configured would be saved and loaded from {USER_CONFIG_PATH}.",
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

    # set value
    setattr(cfg_group, key, value_new)

    # show value
    print(f"{section}.{key} = {value_new}", end=" ")
    print("(updated)" if is_changed else "(not changed)")

    # write config file
    if is_changed:
        try:
            with open(USER_CONFIG_PATH, "w") as fp:
                json.dump(cfg_root.to_dict(), fp, indent=2)
        except Exception:
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
