Plugins
=======

Typed namespace
---------------

.. autoclass:: livy.cli.submit::PreSubmitArguments
   :exclude-members: __init__, __new__

   .. autoattribute:: livy.cli.submit::PreSubmitArguments.script
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.class_name
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.jars
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.py_files
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.files
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.archives
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.queue_name
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.session_name
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.api_url
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.driver_memory
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.driver_cores
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.executor_memory
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.executor_cores
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.num_executors
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.spark_conf
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.watch_log
   .. autoattribute:: livy.cli.submit::PreSubmitArguments.time_prog_start

.. autoclass:: livy.cli.submit::TaskEndedArguments
   :exclude-members: __init__, __new__

   .. autoattribute:: livy.cli.submit::TaskEndedArguments.batch_id
   .. autoattribute:: livy.cli.submit::TaskEndedArguments.state
   .. autoattribute:: livy.cli.submit::TaskEndedArguments.time_task_submit
   .. autoattribute:: livy.cli.submit::TaskEndedArguments.time_task_ended
