Read log
========

``read-log`` is a tool for reading/monitoring logs of specific batch. By default, it would keep tracking for logs until the batch is finished.


Usage
-----

.. program-output:: livy read-log -h


Configurations
--------------

Following configs could be set via *livy config* command:

root.api_url
   URL to Livy server

read_log.keep_watch
   To keep watching or not by default. Could be override by ``--keep-watch`` and ``--no-keep-watch`` argument.
