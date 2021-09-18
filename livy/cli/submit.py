"""Submit a batch task to livy server."""
import argparse
import importlib
import re
import datetime
import typing

import livy
import livy.cli.config
import livy.cli.logging

logger = livy.cli.logging.get(__name__)


def main(argv=None):
    """CLI entrypoint"""
    # parse argument
    cfg = livy.cli.config.load()
    parser = argparse.ArgumentParser(
        prog="livy-submit",
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
        "--class",
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
        "--queue",
        metavar="DEFAULT",
        help="The name of the YARN queue to which submitted",
    )
    parser.add_argument(
        "--name",
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
    g.set_defaults(keep_watch=cfg.submit.watch_log)
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

    args = parser.parse_args(argv)

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
            args = func(args)
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

    # timing
    tzlocal = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    start_time = datetime.datetime.now().astimezone(tzlocal)
    console.debug("Current time= %s", start_time)

    # submit
    # TODO watch log

    return 0


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


if __name__ == "__main__":
    exit(main())
