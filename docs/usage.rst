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

TODO


As library
----------

We could utilize the core components in another scripts. They do not use any extra dependency and could be retrieved by importting ``livy`` package.

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
