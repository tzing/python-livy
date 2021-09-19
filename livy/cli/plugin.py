"""Plugins to use with CLI tasks. Currently only submit task has a hook for
utilizing this.

For a task that uses the hook, it would search for the func pointer by the given
module and function name, then pass the parsed CLI arguments
(`argparse.Namespace` instance) to the function. The deligated function could
do what ever it want, including overwrite the arguments to impact the behavior,
except logging which is already configured before the hook, and return back the
namespace back to pass to next plugins or used in the main function.
"""
import datetime
import json
import logging
import os
import pathlib
import re
import typing
import uuid

if typing.TYPE_CHECKING:
    import argparse


logger = logging.getLogger(__name__)


def read_plugin_config(name: str) -> dict:
    """Serialize configs from the user config file"""
    import livy.cli.config

    # load file
    try:
        with open(livy.cli.config.USER_CONFIG_PATH) as fp:
            all_config = json.load(fp)
    except (FileNotFoundError, json.JSONDecodeError):
        logger.exception("Failed to read user config.")
        return

    cfg = all_config.get(f"plugin:{name}")
    if not cfg:
        logger.error("Configuration for plugin `%s` not exists", name)

    return cfg


def upload_s3(source: str, args: "argparse.Namespace") -> "argparse.Namespace":
    """Pre-submit action to upload local file to AWS S3, so hadoop could read
    the files. It could be useful if you are using this tool with EMR.

    This plugin REQUIRES configurations section `plugin:upload_s3` set in user
    config file. Following key are read:
    - `bucket`: Required key. S3 bucket name
    - `folder_format`: Required key. S3 prefix name format to store the files; it would be
                       expanded inside the plugin with variables `time` (current
                       time), `uuid` (random genersted uuid) or `script_name`
                       (base name part of main application script).
    - `expire_days`: Optional key. Would set object expire date if given.

    Example config:
    ```json
    {
        "plugin:upload_s3": {
            "bucket": "example-bucket",
            "folder_format": "ManualSubmitTask-{time:%Y%m%d}-{uuid}",
            "expire_days": 3
        }
    }
    ```
    """
    if source != "PRE-SUBMIT":
        raise ValueError("this plugin is for pre-submit hook only")

    # get configs
    meta = read_plugin_config("upload_s3")
    assert isinstance(meta, dict), "Config not found"
    logger.debug("Get config: %s", json.dumps(meta))

    bucket = meta["bucket"]
    folder = meta["folder_format"]
    expire_days = meta.get("expire_days")
    assert isinstance(bucket, str)
    assert isinstance(folder, str)

    # shared parameters
    basic_param = {
        "Bucket": bucket,
    }

    if expire_days is None:
        ...  # do nothing
    elif isinstance(expire_days, int) and expire_days > 0:
        expire_date = datetime.datetime.utcnow() + datetime.timedelta(days=expire_days)
        basic_param["Expires"] = expire_date
    else:
        logger.warning("`expire_days` must be a positive integer. got %s", expire_days)
        logger.warning("Continuing the process without setting expire date.")

    # prefix
    script_name, _ = os.path.splitext(os.path.basename(args.script))
    folder = folder.strip("/").format(
        time=datetime.datetime.now(),
        uuid=uuid.uuid4(),
        script_name=script_name,
    )

    logger.debug("Uploading files to s3://%s/%s", bucket, folder)

    # upload to s3
    import boto3

    client = boto3.client("s3")

    def _upload(prefix: str, filepath: str):
        # skip if file is already on s3
        if re.match(r"s3://", filepath, re.RegexFlag.IGNORECASE):
            return filepath

        # build key
        filename = pathlib.Path(filepath).resolve()
        key = os.path.join(folder, prefix, os.path.basename(filepath))
        logger.debug("Upload %s -> s3://%s/%s", filename, bucket, key)

        param = basic_param.copy()
        param.update(
            Filename=filepath,
            Key=key,
        )

        # upload
        client.upload_file(**param)

        return f"s3://{bucket}/{key}"

    args.script = _upload("", args.script)

    if args.jars:
        args.jars = [_upload("jars", fn) for fn in args.jars]
    if args.py_files:
        args.py_files = [_upload("py_files", fn) for fn in args.py_files]
    if args.files:
        args.files = [_upload("files", fn) for fn in args.files]
    if args.archives:
        args.archives = [_upload("archives", fn) for fn in args.archives]

    return args
