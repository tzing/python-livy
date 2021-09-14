"""Submit a batch task to livy server."""
import argparse

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

    group = parser.add_argument_group("livy server configuration")
    group.add_argument(
        "--api-url",
        required=cfg.root.api_url is None,
        default=cfg.root.api_url,
        help="Base-URL for Livy API server",
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


if __name__ == "__main__":
    exit(main())
