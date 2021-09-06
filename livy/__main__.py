import argparse
import sys

import livy.cli.config


def main():
    # parse args
    parser = argparse.ArgumentParser(prog="livy", description="Livy interaction tool")
    subparsers = parser.add_subparsers(title="Sub-command", dest="subcommand")
    subparsers.add_parser("config", help="Get or set configuration")

    args, remain = parser.parse_known_args(sys.argv[1:2])
    remain += sys.argv[2:]

    if args.subcommand is None:
        print("Sub-command is required")
        return 1

    # pass remain argv to subcommand
    _ENTRYPOINT = {
        "config": livy.cli.config.main,
    }

    func = _ENTRYPOINT[args.subcommand]
    return func(remain)


if __name__ == "__main__":
    exit(main())
