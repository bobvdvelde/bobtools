#!/usr/bin/env python

from setuptools import setup

setup(
    name="bobtools",
    version="0.1",
    description="A set of convenience functions for Python.",
    author="Bob van de Velde",
    author_email="bobtools@rnvdv.com",
    url="tba",
    packages=["bobtools"],
    install_requires=["requests", "pandas", "cloudpickle"],
)
