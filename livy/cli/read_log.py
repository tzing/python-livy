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

    parser.add_argument(
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

    livy.cli.logging.setup_argparse(parser)

    args = parser.parse_args(argv)

    # setup logger
    livy.cli.logging.init(args)
    console = livy.cli.logging.get("livy-read-log.main")

    console.info("Reading logs from batch %d", args.batch_id)

    # check batch status
    client = livy.LivyClient(url=args.api_url)

    try:
        client.check(captue=False)
    except livy.RequestError as e:
        console.error(
            "Failed to check batch status. HTTP code=%d, Reason=%s", e.code, e.reason
        )
        return 1

    # fetch log
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
