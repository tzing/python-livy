.. _plugin:

Plugin system
=============

Some of the operation might not be common sense to other user, so these actions are moved to plugins and trigger via the hook. Currently only ``pre-submit`` hook is implemented.


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

It takes following arguments:

* ``source`` the hook name, for preventing user trigger this function in wrong hook. It is hardcoded string and always in uppercase, see table below:

  +----------------+-------------------+
  | name           | ``source`` string |
  +================+===================+
  | pre-submit     | ``PRE-SUBMIT``    |
  +----------------+-------------------+

* ``args`` the argument object, this function could do what they want and modify its value.

And returns :py:class:`~argparse.Namespace` object back to parent function.

For developing, we could utilizing :ref:`plugin-typed-namespace` for type hinting.


Builtin plugin
--------------

.. currentmodule:: livy.cli.plugin
.. autofunction:: upload_s3


.. _plugin-typed-namespace:

Typed namespace
---------------

.. autoclass:: livy.cli.submit.PreSubmitArguments
   :exclude-members: __init__, __new__
