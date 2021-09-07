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


## Usage

#### Use as library

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
