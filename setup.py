#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="bobtools",
    version="0.1",
    description="An eclectic set of convenience functions for Python.",
    author="Bob van de Velde",
    author_email="bobtools@rnvdv.com",
    url="tba",
    packages=find_packages(),
    install_requires=["requests", "pandas", "cloudpickle"],
)
