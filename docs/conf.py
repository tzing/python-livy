# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------
import datetime
import pathlib
import sys

docdir = pathlib.Path(__file__).resolve().parent
repodir = docdir.parent
sys.path.insert(0, str(repodir))

import livy


# -- Project information -----------------------------------------------------
project = "python-livy"
year = datetime.date.today().year
copyright = f"{year}, tzing"
author = "tzing"
version = livy.__version__


# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- AutoDoc -----------------------------------------------------------------
autodoc_class_signature = "separated"
autodoc_typehints = "description"


# -- HTML --------------------------------------------------------------------
html_theme = "pydata_sphinx_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

html_theme_options = {
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/tzing/python-livy",
            "icon": "fab fa-github-square",
        }
    ],
    "show_toc_level": 1,
}
