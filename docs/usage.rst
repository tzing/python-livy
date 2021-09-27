Usage
=====

Requirement
~~~~~~~~~~~

*python-livy* requires Python 3.6+.

It does not require third party library for core features, but could be more handful to use with some optional dependencies. See :ref:`install`.

.. _install:

Install
~~~~~~~

This package is only hosted on Github.

``pip`` does have feature to support downloading packages from git server, so it not exists a strong reason for me to submit this to PyPI.

Use pip
-------

Full installation:

.. code-block:: bash

   pip install 'git+https://github.com/tzing/python-livy.git#egg=livy[pretty,aws]'


Two `"extras" <https://setuptools.pypa.io/en/latest/userguide/dependency_management.html#optional-dependencies>`_ is included, they are for:

pretty
   Enhance the command line output with color and progress bar.

aws
   Install ``boto3`` for using plugin ``upload_s3``.


If you do not need these features, you could use basic installation:

.. code-block:: bash

   pip install 'git+https://github.com/tzing/python-livy.git#egg=livy'


From source
-----------

Please noted the dependencies in this project is managed by `poetry <https://python-poetry.org/docs/>`_.

.. code-block:: bash

   git clone git@github.com:tzing/python-livy.git
   cd python-livy
   poetry install


Quick start
~~~~~~~~~~~

Command line tool
-----------------

First, set the livy server URL in local config file:

.. code-block:: bash

   livy config set root.api_url http://ip-10-12-34-56.us-west-2.compute.internal:8998/

.. note::

   The given URL should not contain extra path (do not include ``/ui/batch``).

All configurations would be saved in ``~/.config/python-livy.json``. Settings could be specificied via argument in each command, but we probably do not want to pass these values every time.

Then we could use it to read logs:

.. code-block:: bash

   livy read-log 1234

For ``read-log`` command, it would keep watching the logs until the batch finished by default. Still, we could turn of this behavior by argument or configuration.

Also, we could ``submit`` a new task:

.. code-block:: bash

   livy submit s3://example-bucket/test_script/main.py

Well, it's bit troublesome to upload the script by our self, so we could utilize the plugin system:

.. code-block:: bash

   livy config set submit.pre_submit livy.cli.plugin:upload_s3

This tool is shipped with plugin ``upload_s3``. It could automatically upload the local scirpt to `AWS S3 <https://aws.amazon.com/s3/>`_. This could be helpful if you are using `EMR <https://aws.amazon.com/emr/>`_.

.. note::

   Currently it does not have plugin for native HDFS / GCP / Azure. Please file an issue or PR if you need it.

This plugin need extra configure but not supporting set via command line. Please use the editor to open ``~/.config/python-livy.json`` and add ``pre-submit:upload_s3`` section:

.. code-block:: json

   {
     "root": {
       "...": "existing configs, please do not change"
     },
     "pre-submit:upload_s3": {
       "bucket": "example-bucket",
       "folder_format": "{time:%Y%m%d%H%M%S}-{script_name}-{uuid}",
       "expire_days": 3
     }
   }

There are three keys: ``bucket`` for S3 bucket name, ``folder_format`` as the prefix to store the scirpt(s), and ``expire_days`` to `set lifetime <https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html>`_ to the objects.

After the configure, we could simply use the command line tool to submit the task:

.. code-block:: bash

   livy submit main.py

Log reader would be started after submission.

.. note::

   ``upload_s3`` plugin uses `boto3 <https://pypi.org/project/boto3/>`_ to upload the files, you should run this tool with ``s3:PutObject``. Or an error would raised.

.. TODO CLI doc

As library
----------

We could utilize the core components in another scripts. They do not use any extra dependency and could be retrieved by importting ``livy`` package.

Note plugin system would not be triggered in core library. For action like *submit*, script(s) should be already stored in somewhere readable by the server.

.. code-block:: python

   >>> import livy
   >>> client = livy.LivyClient("http://ip-10-12-34-56.us-west-2.compute.internal:8998/")
   >>> client.create_batch("s3://example-bucket/test_script/main.py")
   {
       "id": 55,
       "name": None,
       "owner": None,
       "proxyUser": None,
       "state": "starting",
       "appId": None,
       "appInfo": {
           "driverLogUrl": None,
           "sparkUiUrl": None
       },
       "log": [
           "stdout: ",
           "\nstderr: ",
           "\nYARN Diagnostics: "
       ]
   }

   >>> reader = livy.LivyBatchLogReader(client, 55)
   >>> reader.read_until_finish()  # read logs and broadcast to log handlers

for API document, see :ref:`core-lib`.


Advanced usage
~~~~~~~~~~~~~~

Set default configs and repack
------------------------------

In some case, we want to install this tool into multiple environments with setting configurations every time. We could re-packing this tool with default configurations for myself.

First, clone the repo:

.. code-block:: bash

   git clone git@github.com:tzing/python-livy.git
   cd python-livy

Create ``default-configuration.json`` under ``livy/``, this is a hardcoded filename would be read by this tool but not exists in this origin repo.

Save everything we want in this file, could be:

.. code-block:: json

   {
     "root": {
       "api_url": "http://example.com:8998/"
     },
     "submit": {
       "pre_submit": [
         "livy.cli.plugin:upload_s3"
       ]
     },
     "pre-submit:upload_s3": {
       "bucket": "example-bucket",
       "folder_format": "{time:%Y%m%d%H%M%S}-{script_name}-{uuid}"
     }
   }

And build this tool for distributing:

.. code-block:: bash

   poetry build

Then find the wheel or tar file in ``dist/``.
