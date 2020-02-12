#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-swellrewards",
    version="0.1.1",
    description="Singer.io tap for extracting data from Swell Rewards API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_swellrewards"],
    install_requires=[
        "singer-python==5.7.0",
        "requests==2.20.0"
    ],
    entry_points="""
    [console_scripts]
    tap-swellrewards=tap_swellrewards:main
    """,
    packages=["tap_swellrewards"],
    package_data = {
        "schemas": ["tap_swellrewards/schemas/*.json"]
    },
    include_package_data=True,
)
