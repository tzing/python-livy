# Python-Livy

![test status](https://github.com/tzing/python-livy/actions/workflows/test.yml/badge.svg)

> Still under developing. API might not be stable at this time.

Lightweight tool to interact with [Apache Livy](https://livy.incubator.apache.org/). Provide both CLI tools and library that works nicely in pure and native Python.

![screenshot](screenshot.png)

Core features:

1. Native Python

    It uses built in [http.client] for connection. This makes great reduction in both package size and installation time.

2. Full functioned core library

    A client that wraps all batch-related APIs to livy server is provided.

3. Log watching and parsing

    It could parse livy's mixed-stdout logs back to log records and submit to Python's `logging` system. Then we could watch for the events in a much friendly way.

4. Configurable

    For using CLI tools, it provides configuration system for saving common used variables on local storage. No need to set every option on each command.

5. Fully tested

    100% coverage. Both with and without extra dependencies are tested, thanks to Github action.

Extra features, might need extra dependencies:

1. Human friendly log viewer

    On using CLI for reading logs, it could have colored logs and progress bar. Besides, we could highlight or hide specific logger via arguments (e.g. hide `TaskSetManager` as we've got progress bar).

2. Extensible

    Custom function could be triggered during submission. So bring it to infinity...and beyond!


[http.client]: https://docs.python.org/3/library/http.client.html


## Installation

Basic installation:

```bash
pip install 'git+https://github.com/tzing/python-livy.git#egg=livy'
```

For extra features, install with dependencies set `pretty`:

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
