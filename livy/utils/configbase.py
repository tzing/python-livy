"""Config utility provides an interface to get typed config values and could
store/read them into an unified file. By default, it reads settings from
``~/.config/python-livy.json``. Then fallback to default value hardcoded in the
code if the key not exists.

If you are going to repack this tool to suit your environment, it is suggested
to add extra file to :py:const:`CONFIG_LOAD_ORDER` and put your settings inside.
It could be easier to maintain the configuration rather than change every thing
in the code.
"""
import abc
import json
import logging
import pathlib
import typing


__all__ = ["ConfigBase"]

USER_CONFIG_PATH = pathlib.Path.home() / ".config" / "python-livy.json"
CONFIG_LOAD_ORDER = [
    USER_CONFIG_PATH,
]


_T = typing.TypeVar("_T")


class ConfigBase(abc.ABC):
    """Base class for setting configurations. Inspired by
    `pydantic <https://pydantic-docs.helpmanual.io/>`_.
    Thought this, please just treat this class as a simplified version of
    :py:mod:`dataclasses`.

    This class does lake of validation, it is created to perform dictionary
    updating liked config merge and json transform.

    To use this base, inherit this class and use
    `type annotation <https://docs.python.org/3/glossary.html#term-variable-annotation>`_
    to define the fields. Fields would be automatically created during
    :py:meth:`__init__`.
    """

    __missing = object()

    def __init__(self, **kwargs) -> None:
        """
        Parameters
        ----------
        **kwargs
            Field name and values. It would fill with default value defined in
            annotations if not specific, or set to ``None`` when default is not
            set.
        """
        cls = type(self)
        for name, dtype in cls.__annotations__.items():
            # get field value
            value = kwargs.get(name, self.__missing)
            if value is not self.__missing:  # user do assign the value
                ...
            elif isinstance(dtype, type) and issubclass(dtype, ConfigBase):
                # is nested config
                value = dtype()
            elif name in cls.__dict__:
                # get default value from class annotation
                value = cls.__dict__[name]
            else:
                # set none since default is not set
                value = None

            # set value
            self.__dict__[name] = value

    def __repr__(self) -> str:
        class_name = type(self).__name__
        field_values = []
        for k, v in self.__dict__.items():
            field_values.append(f"{k}={v}")
        return f"{class_name}({', '.join( field_values)})"

    def mergedict(self, data: dict) -> None:
        """Merge configs. Overwrite all the value in this instance from a dict.

        Parameters
        ----------
        data : dict
            A dict to provide values.
        """
        assert isinstance(data, dict)
        for field in self.__annotations__:
            value = data.get(field, self.__missing)
            if value is self.__missing:
                continue
            self.__dict__[field] = value

    @classmethod
    def load(cls: typing.Type[_T], section: str) -> _T:
        """Create a config instance and load settings from files. It reads the
        config from :py:const:`CONFIG_LOAD_ORDER`.

        Parameters
        ----------
        section : str
            Key name for reading config

        Return
        ------
        config : ConfigBase
            Config instance with data loaded from the file.
        """
        assert isinstance(section, str)

        inst: ConfigBase = cls()
        for filename in CONFIG_LOAD_ORDER:
            # read file
            try:
                with open(filename, "rb") as fp:
                    data = json.load(fp)
            except FileNotFoundError:
                continue
            except json.JSONDecodeError as e:
                logging.getLogger(__name__).warning(
                    "Decode error in config file %s: %s", filename, e
                )
                continue

            # merge config
            # ignore when section not exists
            if section in data:
                inst.mergedict(data[section])

        return inst
