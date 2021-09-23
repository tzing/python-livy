"""Kill a existing batch.
"""
import argparse
import importlib
import json
import sys
import time

import livy.cli.config
import livy.cli.logging

logger = livy.cli.logging.get(__name__)


def main(argv=None):
    """CLI entrypoint"""
    # parse argument
    cfg = livy.cli.config.load()
    parser = argparse.ArgumentParser(
        prog="livy kill",
        description=__doc__,
    )

    parser.add_argument(
        "batch_id",
        metavar="N",
        type=int,
        help="Livy batch ID for fetching logs",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Proceed the command without showing the prompt",
    )

    group = parser.add_argument_group("livy server configuration")
    group.add_argument(
        "--api-url",
        required=cfg.root.api_url is None,
        default=cfg.root.api_url,
        help="Base-URL for Livy API server",
    )

    livy.cli.logging.setup_argparse(parser)

    args = parser.parse_args(argv)

    # setup logger
    livy.cli.logging.init(args)
    console = livy.cli.logging.get("livy-kill.main")

    # get batch info
    console.info("Connecting to server: %s", args.api_url)

    client = livy.LivyClient(url=args.api_url)

    try:
        batch = client.get_batch_information(args.batch_id)
    except livy.RequestError as e:
        console.error(
            "Failed to check batch status. HTTP code=%d, Reason=%s", e.code, e.reason
        )
        return 1
    except KeyboardInterrupt:
        console.warning("Keyboard interrupt")
        return 1

    console.info(
        "Batch #%d information: %s", args.batch_id, json.dumps(batch, indent=2)
    )

    if batch["state"] not in ("starting", "running"):
        console.warning("Task is already ended.")
        return 1

    # double confirm
    time.sleep(0.1)  # wait for previous logs
    if not args.yes and not check_user_confirm(args.batch_id):
        console.warning("User cancellation")
        return 1

    # kill task
    console.info("Send kill request")

    try:
        client.delete_batch(args.batch_id)
    except livy.RequestError as e:
        console.error("Failed to kill task. HTTP code=%d, Reason=%s", e.code, e.reason)
        return 1
    except KeyboardInterrupt:
        console.warning("Keyboard interrupt")
        return 1

    # keep monitor status
    console.info("Monitor task status")

    while True:
        try:
            is_finished = client.is_batch_ended(args.batch_id)
        except livy.RequestError as e:
            console.error(
                "Failed to get batch status. HTTP code=%d, Reason=%s", e.code, e.reason
            )
            return 1
        except KeyboardInterrupt:
            console.warning("Keyboard interrupt")
            return 1

        if is_finished:
            console.info("Task terminated")
            break

        console.info("Task is still running")
        time.sleep(2.0)

    return 0


def check_user_confirm(id_: int) -> True:
    """Show prompt to ask if user really want to kill the task.

    Parameters
    ----------
        id_ : int
            Batch id

    Return
    ------
    yes : bool
        Yes user want to kill this batch
    """
    # enable colors if possible
    try:
        colorama = importlib.import_module("colorama")
        GREEN = colorama.Fore.GREEN
        BRIGHT = colorama.Style.BRIGHT
        DIM = colorama.Style.DIM
        RESET = colorama.Style.RESET_ALL
    except ImportError:
        GREEN = BRIGHT = DIM = RESET = ""

    # show prompt
    print(
        f"{BRIGHT}=> Kill batch {GREEN}#{id_}{RESET}{BRIGHT}? "
        f"{DIM}[ No / yes ] {RESET}",
        file=sys.stdout,
        end="",
    )
    sys.stdout.flush()

    # get user response
    answer = input().strip()
    if not answer:
        return False

    if answer.upper() in ("Y", "YES"):
        return True
    elif answer.upper() in ("N", "NO"):
        return False
    else:
        logger.error("Invalid input. Should be `yes` or `no`.")
        return False


if __name__ == "__main__":
    exit(main())
