import argparse
import os
import tempfile
import unittest
import unittest.mock

import livy.cli.logging as module


class TestArgumentParse(unittest.TestCase):
    def tearDown(self) -> None:
        module._is_initialized = False

    def test_setup_argparse(self):
        p = argparse.ArgumentParser()
        module.setup_argparse(p)
        p.parse_args(["-q"])

    def test_init(self):
        with tempfile.NamedTemporaryFile() as fp, unittest.mock.patch(
            "os.getcwd", return_value=os.path.dirname(fp.name)
        ):
            module.init()

        module.init()  # test cache

    def test_init_with_display(self):
        args = argparse.Namespace()
        args.verbose = 0
        args.log_file = True
        args.highlight_logger = ["test-highlight-logger"]
        args.hide_logger = ["test-hide-logger"]
        args.with_progressbar = False

        # fmt: off
        with tempfile.NamedTemporaryFile() as fp, \
            unittest.mock.patch("os.getcwd", return_value=os.path.dirname(fp.name)):
            module.init(args)
        # fmt: on
