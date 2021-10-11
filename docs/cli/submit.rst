.. _cli-submit:

Submit
======

``submit`` is design to create a `batch <https://livy.incubator.apache.org/docs/latest/rest-api.html#Batch>`_ on livy server. It sends request to livy, get response and start watching for logs (like what's happening in :ref:`cli-read-log`).

In original use case, I'm designing and utilizing this tool for sending local on to the server as a test. The local files are of course not accessible to the livy server. Therefore it comes with :ref:`plugin` system for automatically upload the file to somewhere avaliable to the server.


Usage
-----

.. program-output:: livy submit -h


Configurations
--------------

Following configs could be set via :ref:`cli-config` command:

root.api_url
   URL to Livy server

submit.pre_submit
   List of plugin to be triggered before task is submitted to server. Value should be in ``module1:func,module2:func2`` format. e.g. ``livy.cli.plugin:upload_s3`` would bypass the meta to :py:func:`upload_s3` in :py:mod:`livy.cli.plugin` module.

submit.driver_memory
   Amount of memory to use for the driver process. Need to specific unit, e.g. ``12gb`` or ``34mb``.

submit.driver_cores
   Number of cores to use for the driver process.

submit.executor_memory
   Amount of memory to use per executor process. Need to specific unit, e.g. ``12gb`` or ``34mb``.

submit.executor_cores
   Number of cores to use for each executor.

submit.num_executors
   Number of executors to launch for this batch.

submit.spark_conf
   Key value pairs to override spark configuration properties.

submit.watch_log
   Watching for logs after the task is submitted. This option shares the same behavior to :py:attr:`~ReadLogSection.keep_watch`, only different is the scope it take effects.
