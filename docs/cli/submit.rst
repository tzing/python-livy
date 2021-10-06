Submit
======

``submit`` is design to create a `batch <https://livy.incubator.apache.org/docs/latest/rest-api.html#Batch>`_ on livy server. It sends request to livy, get response and start watching for logs (like what's happening in :ref:`cli-read-log`).

In original use case, I'm designing and utilizing this tool for sending local on to the server as a test. The local files are of course not accessible to the livy server. Therefore it comes with :ref:`plugin` system for automatically upload the file to somewhere avaliable to the server.


Usage
-----

.. program-output:: livy submit -h
