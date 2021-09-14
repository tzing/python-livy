"""Read livy batch execution log from server
"""
import argparse
import logging

import livy
import livy.cli.config
import livy.cli.logging


def main(argv=None):
    """CLI entrypoint"""
    # parse argument
    cfg = livy.cli.config.load()
    parser = argparse.ArgumentParser(
        prog="livy-read-log",
        description=__doc__,
    )

    parser.add_argument(
        "batch_id",
        metavar="N",
        type=int,
        help="Livy batch ID for fetching logs",
    )

    group = parser.add_argument_group("Livy server configuration")
    group.add_argument(
        "--api-url",
        required=cfg.root.api_url is None,
        default=cfg.root.api_url,
        help="Base-URL for Livy API server",
    )

    g = parser.add_mutually_exclusive_group()
    g.set_defaults(keep_watch=cfg.read_log.keep_watch)
    g.add_argument(
        "--keep-watch",
        dest="keep_watch",
        action="store_true",
        help="Keep watching this batch until it is finished",
    )
    g.add_argument(
        "--no-keep-watch",
        dest="keep_watch",
        action="store_false",
        help="Only read log once",
    )

    livy.cli.logging.setup_argparse(parser, True)

    args = parser.parse_args(argv)

    # setup logger
    livy.cli.logging.init(args)
    console = livy.cli.logging.get("livy-read-log.main")

    # check batch status
    console.info("Connecting to server: %s", args.api_url)

    client = livy.LivyClient(url=args.api_url)

    try:
        is_finished = client.is_batch_finished(args.batch_id)
    except livy.RequestError as e:
        console.error(
            "Failed to check batch status. HTTP code=%d, Reason=%s", e.code, e.reason
        )
        return 1
    except KeyboardInterrupt:
        console.warning("Keyboard interrupt")
        return 1

    # fetch log
    console.info("Reading logs from batch %d", args.batch_id)
    if is_finished:
        args.keep_watch = False
        console.warning(
            "Batch %d is already finished. Disable keep-watch behavior.", args.batch_id
        )

    reader = livy.LivyBatchLogReader(client, args.batch_id)

    if args.keep_watch:
        read_func = reader.read_until_finish
    else:
        read_func = reader.read

    try:
        read_func()
    except livy.RequestError as e:
        console.error(
            "Error occurs during read log. HTTP code=%d, Reason=%s", e.code, e.reason
        )
        return 1
    except KeyboardInterrupt:
        console.warning("Keyboard interrupt")
        return 1

    # finish
    if args.keep_watch:
        state = client.get_batch_state(args.batch_id)
        level = logging.INFO if state == "success" else logging.WARNING
        console.log(level, "Batch finished with state=%s", state)

    return 0


if __name__ == "__main__":
    exit(main())
