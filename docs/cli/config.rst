.. _cli-config:

Configure
=========

``config`` is a command to get or set the configuration. This command is underlying on :ref:`config-base`, it saves the values on local directory and let the corresponding command filled with the pre-configured settings.

Usage
-----

List all configurable keys
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. program-output:: livy config list -h

It provides a command to view the list, but is without description. For the details, see below.

Get value
~~~~~~~~~

.. program-output:: livy config get -h

Set value
~~~~~~~~~

.. program-output:: livy config set -h

Not all of used values could be set via this command. For example, values used :ref:`plugin` should be inputted manually.


All configurable keys
---------------------

A valid config name for the CLI tool should be in ``prefix.name`` format, the *prefix* would be placed in class description and *name* is the attribute name in each of config class. For example, ``root.api_url`` would be a valid key for CLI tool.

The value shows after the attribute is default value when it is not override by user. Some of them could be ``None``.

.. autoclass:: livy.cli.config::RootSection
   :exclude-members: __init__, __new__

   .. autoattribute:: livy.cli.config::RootSection.api_url

.. autoclass:: livy.cli.config::LocalLoggingSection
   :exclude-members: __init__, __new__

   .. autoattribute:: livy.cli.config::LocalLoggingSection.format
   .. autoattribute:: livy.cli.config::LocalLoggingSection.date_format
   .. autoattribute:: livy.cli.config::LocalLoggingSection.output_file
   .. autoattribute:: livy.cli.config::LocalLoggingSection.logfile_level
   .. autoattribute:: livy.cli.config::LocalLoggingSection.with_progressbar

.. autoclass:: livy.cli.config::ReadLogSection
   :exclude-members: __init__, __new__

   .. autoattribute:: livy.cli.config::ReadLogSection.keep_watch

.. autoclass:: livy.cli.config::SubmitSection
   :exclude-members: __init__, __new__

   .. autoattribute:: livy.cli.config::SubmitSection.pre_submit
   .. autoattribute:: livy.cli.config::SubmitSection.driver_memory
   .. autoattribute:: livy.cli.config::SubmitSection.driver_cores
   .. autoattribute:: livy.cli.config::SubmitSection.executor_memory
   .. autoattribute:: livy.cli.config::SubmitSection.executor_cores
   .. autoattribute:: livy.cli.config::SubmitSection.num_executors
   .. autoattribute:: livy.cli.config::SubmitSection.spark_conf
   .. autoattribute:: livy.cli.config::SubmitSection.watch_log
