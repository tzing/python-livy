"""Submit a batch task to livy server."""
import argparse
import re

import livy
import livy.cli.config
import livy.cli.logging


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

    livy.cli.logging.setup_argparse(parser, True)

    args = parser.parse_args(argv)

    # TODO
    raise NotImplementedError()


def argmem(s: str):
    """Validate input for memory size"""
    if not re.fullmatch("\d+[gm]b?", s, re.RegexFlag.IGNORECASE):
        raise argparse.ArgumentTypeError(s)
    return s


def argkvpair(val):
    """Splitting key value pair"""
    k, v = val.split("=", 1)
    return k, v


if __name__ == "__main__":
    exit(main())
