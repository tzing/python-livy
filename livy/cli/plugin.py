import typing

if typing.TYPE_CHECKING:
    import argparse


def upload_s3(args: "argparse.Namespace") -> "argparse.Namespace":
    """Pre-submit action to upload local file to AWS S3, so livy and EMR could
    be able to read this file."""

    return args
