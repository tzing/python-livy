Installation
============

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
