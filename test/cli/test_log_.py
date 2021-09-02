import argparse
import logging
import unittest.mock

import livy.cli.log as module


def test_setup_argparse():
    p = argparse.ArgumentParser()
    module.setup_argparse(p)
    p.parse_args(["-q"])


def test_init():
    args = argparse.Namespace()
    args.verbose = 0
    module.init(args)
    module.init(args)


@unittest.mock.patch("livy.cli.log._use_color_handler", return_value=False)
def test__get_console_formatter_1(_):
    assert isinstance(module._get_console_formatter(), logging.Formatter)


@unittest.mock.patch("livy.cli.log._use_color_handler", return_value=True)
def test__get_console_formatter_2(_):
    assert isinstance(module._get_console_formatter(), logging.Formatter)


def test_get():
    assert isinstance(module.get(), logging.Logger)
