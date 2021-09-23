import abc


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

    def merge(self, other: "ConfigBase") -> None:
        """Merge configs. Overwrite all the value in this instance from another
        instance once the value is not ``None``.

        Parameter
        ---------
        other : ConfigBase
            Another instance to provide values.
        """
        for field in self.__annotations__:
            value = getattr(other, field, self.__missing)
            if value is self.__missing or value is None:
                continue
            self.__dict__[field] = value
