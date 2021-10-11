Utility
=======

``livy.utils`` is a collection of helper classes to make the CLI tool more friendly, this module is not included in ``livy`` import by default.

Most of the classes in this module need extra dependency to work. If you call them in a non-compatible environment, it would automatically fallback to some builtin python function with the similar features.

.. _config-base:

livy.utils.ConfigBase
---------------------

.. automodule:: livy.utils.configbase

.. autoclass:: livy.utils.configbase.ConfigBase
   :members:

livy.utils.EnhancedConsoleHandler
---------------------------------

.. autoclass:: livy.utils.logging.EnhancedConsoleHandler
   :members:

livy.utils.ColoredFormatter
---------------------------
.. autoclass:: livy.utils.logging.ColoredFormatter
   :members:

livy.utils.IngoreLogFilter
--------------------------

.. autoclass:: livy.utils.logging.IngoreLogFilter
   :members:
