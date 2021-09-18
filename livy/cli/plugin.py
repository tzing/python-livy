"""Plugins to use with CLI tasks. Currently only submit task has a hook for
utilizing this.

For a task that uses the hook, it would search for the func pointer by the given
module and function name, then pass the parsed CLI arguments
(`argparse.Namespace` instance) to the function. The deligated function could
do what ever it want, including overwrite the arguments to impact the behavior,
except logging which is already configured before the hook, and return back the
namespace back to pass to next plugins or used in the main function.
"""
import typing

if typing.TYPE_CHECKING:
    import argparse


def upload_s3(args: "argparse.Namespace") -> "argparse.Namespace":
    """Pre-submit action to upload local file to AWS S3, so hadoop could read
    the files. It could be useful if you are using this tool with EMR."""
    raise NotImplementedError()
