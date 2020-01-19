#!/usr/bin/env python

from setuptools import setup

setup(
    name="bobtools",
    version="0.1",
    description="An eclectic set of convenience functions for Python.",
    author="Bob van de Velde",
    author_email="bobtools@rnvdv.com",
    url="tba",
    package_dir={"": "bobtools"},
    packages=["bobtools"],
    install_requires=["requests", "pandas", "cloudpickle"],
)
