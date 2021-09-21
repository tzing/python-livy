# Python-Livy

![test status](https://github.com/tzing/python-livy/actions/workflows/test.yml/badge.svg)

Library to interact with [Apache livy](https://livy.incubator.apache.org/).

This tool is designed for the entire livy submission task, including creating a new batch and watching for its log until job is ended.

> Under developing: command line interface tool for it

#### Features

* Livy communication

    It implements `LivyClient` that wraps batch-related APIs to livy server.

    All listed API for livy 0.7.0 is ported. Including `create_batch`, `delete_batch`, `get_batch_information`, `get_batch_state` and `get_batch_log`.

* Log watching and parsing

    Class `LivyBatchLogReader` could be helpful to fetch logs from livy server.

    Once a new log is observed, it would parse it and publish into Python's `logging` system. Then we could use other familiar library/functions to handle the logs.


## Requirement

Python >= 3.6. No third-party library is required for core features.


## Installation

Basic installation:

```bash
pip install 'git+https://github.com/tzing/python-livy.git#egg=livy'
```

If you're using CLI, we could have a better view by install with extra dependencies:

```bash
pip install 'git+https://github.com/tzing/python-livy.git#egg=livy[pretty]'
```


## Usage

### Command line tool

```bash
# save common used variable via `livy config`
livy config set root.api_url http://ip-10-12-34-56.us-west-2.compute.internal:8998/

# submit a task
# Note: By default you should add a path that is readable by the hadoop server.
#       But we could simplified the step by using plugin, see below for more details.
livy submit hdfs://host:port/file

# read logs from specific batch
livy read-log 1234

# kill a task
livy kill 1234
```

#### Extra: Use with AWS Elastic MapReduce (EMR)

It has a plugin to automatically upload local file to a S3 bucket.
To enable this feature, config the `pre-submit` hook list:

```bash
livy config set submit.pre_submit livy.cli.plugin:upload_s3
```

And add following section to `~/.config/python-livy.json`:

```json
{
  "plugin:upload_s3": {
    "bucket": "example-bucket",
    "folder_format": "ManualSubmitTask-{time:%Y%m%d}-{uuid}",
    "expire_days": 3
  }
}
```

Please kindly replace the `bucket` to some bucket that is readable to EMR, and configure the `folder_format` with proper string in your environment. The `time` and `uuid` part in `folder_format` would be replaced in runtime.

### Use as library

```python
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
```
