import argparse
import sys
import time

import livy.cli.config
import livy.cli.read_log
import livy.cli.submit
import livy.cli.kill

_ENTRYPOINT = {
    "config": livy.cli.config.main,
    "read-log": livy.cli.read_log.main,
    "submit": livy.cli.submit.main,
    "kill": livy.cli.kill.main,
}


def main():
    # parse args
    parser = argparse.ArgumentParser(prog="livy", description="Livy interaction tool")
    subparsers = parser.add_subparsers(title="Sub-command", dest="subcommand")
    subparsers.add_parser("submit", help=livy.cli.submit.__doc__)
    subparsers.add_parser("read-log", help=livy.cli.read_log.__doc__)
    subparsers.add_parser("kill", help=livy.cli.kill.__doc__)
    subparsers.add_parser("config", help=livy.cli.config.__doc__)

    args, remain = parser.parse_known_args(sys.argv[1:2])
    remain += sys.argv[2:]

    if args.subcommand is None:
        print("Sub-command is required: ", end="")
        print(*subparsers.choices, sep=" / ")
        return 1

    # pass remain argv to subcommand
    func = _ENTRYPOINT[args.subcommand]
    return func(remain)


if __name__ == "__main__":
    # The special logger inside livy.cli.logging has a internal queue for
    # storing the logs and flush it by batch. Sometime the queue isn't empty at
    # the time we exit the progream, and cause multiprocessing resource tracker
    # warning about leaked semaphore. As a workaround, we add sleep here to
    # ensure flush is triggered and queue should be empty.
    code = main()
    time.sleep(0.1)
    exit(code)
