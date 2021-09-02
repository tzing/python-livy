import json
import os
import tempfile

import livy.cli.config as module


def test_load_default():
    # test
    s1 = module.load()
    s2 = module.load()
    assert s1 is s2

    # tear down
    module._settings = None


def test_load_with_config():
    # prepare
    _, path = tempfile.mkstemp()
    with open(path, "w") as fp:
        fp.write(
            json.dumps(
                {
                    "root": {
                        "api_url": "http://example.com/",
                    },
                    "read_log": {
                        "keep_watch": False,
                    },
                }
            )
        )

    # test
    s = module.load(path)

    assert s.root.api_url == "http://example.com/"
    assert s.read_log.keep_watch == False

    # tear down
    module._settings = None
    os.remove(path)
