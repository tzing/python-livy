"""Submit a batch task to livy server."""
import argparse
import datetime
import importlib
import json
import logging
import re
import typing

import livy
import livy.cli.config
import livy.cli.logging

logger = logging.getLogger(__name__)


class PreSubmitArguments(argparse.Namespace):
    """Typed :py:class:`~argparse.Namespace` for arguments before task submission."""

    # submit
    script: str
    args: typing.List[str]
    class_name: str
    jars: typing.List[str]
    py_files: typing.List[str]
    files: typing.List[str]
    archives: typing.List[str]
    queue_name: str
    session_name: str
    api_url: str
    driver_memory: str
    driver_cores: int
    executor_memory: str
    executor_cores: int
    num_executors: int
    spark_conf: typing.List[typing.Tuple[str, str]]

    # log
    watch_log: bool


def main(argv=None):
    """CLI entrypoint"""
    # parse argument
    cfg = livy.cli.config.load()
    parser = argparse.ArgumentParser(
        prog="livy submit",
        description=__doc__,
    )

    parser.add_argument(
        "script",
        help="Path to the script that contains the application to be executed",
    )
    parser.add_argument(
        "args",
        nargs="*",
        help="Arguments for the task script",
    )

    parser.add_argument(
        "--class-name",
        metavar="COM.EXAMPLE.FOO",
        help="Application Java/Spark main class (for Java/Scala task)",
    )
    parser.add_argument(
        "--jars",
        nargs="+",
        metavar="FOO.JAR",
        help="Java dependencies to be used in this batch",
    )
    parser.add_argument(
        "--py-files",
        nargs="+",
        metavar="FOO.ZIP",
        help="Python dependencies to be used in this batch",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        metavar="FOO.TXT",
        help="Files to be used in this batch",
    )
    parser.add_argument(
        "--archives",
        nargs="+",
        metavar="FOO.TAR",
        help="Archives to be used in this batch",
    )
    parser.add_argument(
        "--queue-name",
        metavar="DEFAULT",
        help="The name of the YARN queue to which submitted",
    )
    parser.add_argument(
        "--session-name",
        metavar="HELLO",
        help="The session name to execute this batch",
    )

    group = parser.add_argument_group("pre-submit actions")
    group.add_argument(
        "--pre-submit",
        metavar="PLUG",
        nargs="+",
        default=cfg.submit.pre_submit,
        help="Run specific plugin before submit",
    )

    group = parser.add_argument_group("livy server configuration")
    group.add_argument(
        "--api-url",
        required=cfg.root.api_url is None,
        default=cfg.root.api_url,
        help="Base-URL for Livy API server",
    )
    group.add_argument(
        "--driver-memory",
        metavar="10G",
        default=cfg.submit.driver_memory,
        type=argmem,
        help="Amount of memory to use for the driver process.",
    )
    group.add_argument(
        "--driver-cores",
        metavar="N",
        default=cfg.submit.driver_cores,
        type=int,
        help="Number of cores to use for the driver process.",
    )
    group.add_argument(
        "--executor-memory",
        metavar="10G",
        default=cfg.submit.executor_memory,
        type=argmem,
        help="Amount of memory to use for the executor process.",
    )
    group.add_argument(
        "--executor-cores",
        metavar="N",
        default=cfg.submit.executor_cores,
        type=int,
        help="Number of cores to use for each executor.",
    )
    group.add_argument(
        "--num-executors",
        metavar="N",
        default=cfg.submit.num_executors,
        type=int,
        help="Number of executors to launch for this batch.",
    )
    group.add_argument(
        "--spark-conf",
        metavar="CONF_NAME=VALUE",
        nargs="+",
        default=cfg.submit.spark_conf,
        type=argkvpair,
        help="Spark configuration properties.",
    )

    group = parser.add_argument_group("post-submit actions")
    g = group.add_mutually_exclusive_group()
    g.set_defaults(watch_log=cfg.submit.watch_log)
    g.add_argument(
        "--watch-log",
        dest="watch_log",
        action="store_true",
        help="Watching for logs until it is finished",
    )
    g.add_argument(
        "--no-watch-log",
        dest="watch_log",
        action="store_false",
        help="Not to watch for logs. Only submit the task and quit.",
    )

    livy.cli.logging.setup_argparse(parser)

    args: PreSubmitArguments = parser.parse_args(argv)

    # setup logger
    livy.cli.logging.init(args)
    console = livy.cli.logging.get("livy-read-log.main")
    console.info("Submission task started")

    # run pre-submit actions
    for action in args.pre_submit:
        console.info("Run pre-submit action %s", action)

        func = get_function(action)
        if not func:
            console.warning("Failed to get action function instance. Stop process.")
            return 1

        try:
            args = func("PRE-SUBMIT", args)
        except:
            console.exception("Error occurs during pre-submit action. Stop process.")
            return 1

        if not isinstance(args, argparse.Namespace):
            console.error(
                "Return value should be a namespace object. Got %s", type(args).__name__
            )
            return 1

    # check server state
    client = livy.LivyClient(url=args.api_url)

    try:
        client.check(False)
    except livy.RequestError as e:
        console.error("Failed to connect to server: %s", e)
        return 1

    # build request payload
    submit_parameter = {}

    for key, value in [
        ("file", args.script),
        ("class_name", args.class_name),
        ("args", args.args),
        ("jars", args.jars),
        ("py_files", args.py_files),
        ("files", args.files),
        ("driver_memory", args.driver_memory),
        ("driver_cores", args.driver_cores),
        ("executor_memory", args.executor_memory),
        ("executor_cores", args.executor_cores),
        ("num_executors", args.num_executors),
        ("archives", args.archives),
        ("queue", args.queue_name),
        ("name", args.session_name),
        ("conf", {k: v for k, v in args.spark_conf}),
    ]:
        if value:
            submit_parameter[key] = value

    console.info(
        "Creating batch with parameters: %s",
        json.dumps(submit_parameter, indent=2),
    )

    # timing
    tzlocal = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    start_time = datetime.datetime.now().astimezone(tzlocal)
    console.debug("Batch submission time= %s", start_time)

    # submit
    try:
        submit_resp = client.create_batch(**submit_parameter)
    except livy.RequestError as e:
        console.error("Failed to connect to server: %s", e)
        return 1

    console.info("Server response: %s", json.dumps(submit_resp, indent=2))

    batch_id: int = submit_resp.get("id", None)
    if not isinstance(batch_id, int) or batch_id < 0:
        console.error("Failed to get batch id. Something goes wrong.")
        return 1

    # watch log
    if not args.watch_log:
        console.info("Batch %d created.", batch_id)
        return 0

    console.info("Start reading logs of batch %d", batch_id)

    reader = livy.LivyBatchLogReader(client, batch_id)

    try:
        reader.read_until_finish()
    except livy.RequestError as e:
        console.error(
            "Error occurs during read log. HTTP code=%d, Reason=%s", e.code, e.reason
        )
        return 1
    except KeyboardInterrupt:
        console.warning("Keyboard interrupt. Local livy-submit process terminating.")
        console.warning("Your task might be still running on the server.")
        console.warning("For reading the logs, call:")
        console.warning("    livy read-log %d --api-url %s", batch_id, args.api_url)
        console.warning("For stopping the task, call:")
        console.warning("    livy kill %d --api-url %s", batch_id, args.api_url)
        return 1

    # timing
    finish_time = datetime.datetime.now().astimezone(tzlocal)
    elapsed_time = finish_time - start_time
    console.debug("Batch finishing time= %s", finish_time)

    # get ending state
    try:
        state = client.get_batch_state(batch_id)
    except livy.RequestError:
        console.error("Error during query batch ending state.")
        return 1

    if state == "success":
        code = 0
        level = logging.INFO
    else:
        code = 1
        level = logging.WARNING

    console.log(level, "Batch ended with state= %s", state)
    console.info(
        "Batch execution time: %dsec (%s)",
        elapsed_time.total_seconds(),
        human_readable_timeperiod(elapsed_time),
    )

    return code


def argmem(s: str):
    """Validate input for memory size"""
    if not re.fullmatch(r"\d+[gm]b?", s, re.RegexFlag.IGNORECASE):
        raise argparse.ArgumentTypeError(
            "please specific memory size in format '1234mb'"
        )
    return s


def argkvpair(val):
    """Splitting key value pair"""
    k, v = val.split("=", 1)
    return k, v


def get_function(name: str) -> typing.Callable:
    """Get function by module name"""
    m = re.fullmatch(r"([\w.]+):(\w+)", name, re.RegexFlag.I)
    if not m:
        logger.error("Failed to resolve function name: %s", name)
        logger.error("Please specific it in module:func format")
        return

    module_name, func_name = m.groups()

    try:
        module = importlib.import_module(module_name)
    except ImportError:
        logger.error("Failed to find module: %s", module_name)
        return

    try:
        func = getattr(module, func_name)
    except AttributeError:
        logger.error("Failed to find function %s in %s", func_name, module_name)
        return

    return func


def human_readable_timeperiod(period: datetime.timedelta):
    """Convert time period to human readable format"""
    total_seconds = int(period.total_seconds())

    terms = []
    days = total_seconds // 86400
    if days:
        terms.append(f"{days}d")

    hours = total_seconds // 3600 % 24
    if hours:
        terms.append(f"{hours}h")

    minutes = total_seconds // 60 % 60
    if minutes:
        terms.append(f"{minutes}m")

    seconds = total_seconds % 60
    if seconds:
        terms.append(f"{seconds}s")

    return " ".join(terms)


if __name__ == "__main__":
    exit(main())
