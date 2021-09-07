import argparse
import importlib.util
import logging
import os
import tempfile
import unittest.mock

import livy.cli.logging as module


def test_setup_argparse():
    p = argparse.ArgumentParser()
    module.setup_argparse(p)
    p.parse_args(["-q"])


@unittest.mock.patch("livy.cli.logging._is_initialized", False)
def test_init():
    args = argparse.Namespace()
    args.verbose = 0
    args.log_file = True

    with tempfile.NamedTemporaryFile() as fp, unittest.mock.patch(
        "os.getcwd", return_value=os.path.dirname(fp.name)
    ):
        module.init(args)

    module.init(args)


def test__get_console_formatter():
    with unittest.mock.patch("livy.cli.logging._use_color_handler", return_value=False):
        assert isinstance(module._get_console_formatter(), logging.Formatter)

    if importlib.util.find_spec("colorlog"):  # test-core does not install colorlog
        with unittest.mock.patch(
            "livy.cli.logging._use_color_handler", return_value=True
        ):
            assert isinstance(module._get_console_formatter(), logging.Formatter)
