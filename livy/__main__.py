import argparse
import sys

import livy.cli.config
import livy.cli.read_log
import livy.cli.submit

_ENTRYPOINT = {
    "config": livy.cli.config.main,
    "read-log": livy.cli.read_log.main,
    "submit": livy.cli.submit.main,
}


def main():
    # parse args
    parser = argparse.ArgumentParser(prog="livy", description="Livy interaction tool")
    subparsers = parser.add_subparsers(title="Sub-command", dest="subcommand")
    subparsers.add_parser("submit", help=livy.cli.submit.__doc__)
    subparsers.add_parser("read-log", help=livy.cli.read_log.__doc__)
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
    exit(main())
