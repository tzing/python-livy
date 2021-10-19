.. _plugin:

Plugin system
=============

Some of the operation might not be common sense to other user, so these actions are moved to plugins and trigger via the hook.


How to use it
-------------

To enable a plugin, we could setup via :ref:`cli-config` or override it with corresponding argument. The parent function/tool pass the :py:class:`~argparse.Namespace` object to the deligated function, and use the returned one in the rest part of functions.

The deligated function could do what ever it want, including overwrite the arguments to impact the behavior, except logging behavior, which is already configured before the hook.

Take ``pre-submit`` hook for example, it could be set in :py:const:`~livy.cli.config.SubmitSection.pre_submit`, or use ``--pre-submit`` in :ref:`cli-submit`. A :py:class:`~livy.cli.submit.PreSubmitArguments` would be given to the plugin function and the it could run actions and override the parameters.


Create your own plugin
----------------------

Plugins are importted and triggered in runtime. Therefore you could place your functions everywhere, you only need to ensure it is under your `PYTHONPATH <https://docs.python.org/3/using/cmdline.html#envvar-PYTHONPATH>`_.

A plugin function must in following signature:

.. code-block::

   def action(source: str, args: argparse.Namespace) -> argparse.Namespace:
       ...

It takes arguments ``source`` and ``args``, and returns :py:class:`~argparse.Namespace` object back to parent function.

Input argument ``source`` the hook name, for preventing user trigger this function in wrong hook. It is hardcoded string and always in uppercase, see table below for details. Argument ``args`` an :py:class:`~argparse.Namespace` object, the plugin function could do what they want and modify its value.

Expected input value and typed namespace instances are:

.. table::
   :widths: 30 20 50

   +----------------+-------------------+-------------------------------------------------+
   | name           | ``source`` string | Typed namespace reference                       |
   +================+===================+=================================================+
   | pre-submit     | ``PRE-SUBMIT``    | :py:class:`~livy.cli.submit.PreSubmitArguments` |
   +----------------+-------------------+-------------------------------------------------+
   | task-success   | ``TASK-SUCCESS``  | :py:class:`~livy.cli.submit.PreSubmitArguments` |
   +----------------+-------------------+-------------------------------------------------+
   | task-failed    | ``TASK-FAILED``   | :py:class:`~livy.cli.submit.PreSubmitArguments` |
   +----------------+-------------------+-------------------------------------------------+
   | task-ended     | ``TASK-ENDED``    | :py:class:`~livy.cli.submit.TaskEndedArguments` |
   +----------------+-------------------+-------------------------------------------------+


Builtin plugin
--------------

.. currentmodule:: livy.cli.plugin

Upload script to S3
+++++++++++++++++++

.. autofunction:: upload_s3
