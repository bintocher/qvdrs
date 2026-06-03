# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
#
# IMPORTANT: the Python API is provided by the compiled PyO3 module `qvd`.
# Before running `sphinx-build`, install the module into the active Python
# environment with:
#
#     maturin develop --features python
#
# Without this step, `autodoc` cannot import `qvd` and the API reference
# will be empty.

# -- Project information -----------------------------------------------------
#
# Pulled from the repo's pyproject.toml so name / version / author stay in one
# place. Requires Python 3.11+ (tomllib). conf.py lives at
# docs/python/source/conf.py, so the repo root is four parents up.

import tomllib
from datetime import date
from pathlib import Path

_pyproject = tomllib.loads(
    (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text(encoding="utf-8")
)
_project_meta = _pyproject["project"]
_authors = ", ".join(a["name"] for a in _project_meta.get("authors", []) if "name" in a)

project = _project_meta["name"]
release = _project_meta["version"]
author = _authors
copyright = f"{date.today().year}, {_authors}"

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
]

templates_path = ["_templates"]
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
