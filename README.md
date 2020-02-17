# Bobtools: Some simple convenience functions for Python 3.7+

![Python package](https://github.com/bobvdvelde/bobtools/workflows/Python%20package/badge.svg?branch=master)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This package is still very rough. Currently implemented:

- `bobtools.parallel.funnel` : an easy fan-out, fan-in function for parallel processing of streams. 
- `bobtools.datascan.dictscanner` : An object to extract the schema and prototype from (nested) dictionary data.
- `bobtools.io.JSONL` : A class to easily read & write (gzipped) one-by-line json objects to disk. 