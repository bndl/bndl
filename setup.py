#!/usr/bin/env python

import os

from setuptools import setup, find_packages
from distutils.extension import Extension
import re

try:
    from Cython.Build.Dependencies import cythonize
    USE_CYTHON = True
except ImportError:
    USE_CYTHON = False


ext = re.compile(
    '\.%s$' % ('pyx' if USE_CYTHON else 'c')
)

extensions = [
    Extension(
        '.'.join((root.replace(os.sep, '.'), ext.sub('', f))),
        [os.path.join(root, f)]
    )
    for root, dirs, files in os.walk('bndl')
    for f in files
    if not root.endswith('tests')
    if ext.search(f)
]


if USE_CYTHON:
    extensions = cythonize(extensions)

setup(
    name="bndl",
    version="0.0.1",
    url="",
    description="Bundle compute resources with BNDL",
    author="Frens Jan Rumph",
    author_email="mail@frensjan.nl",

    packages=(
        find_packages()
    ),

    include_package_data=False,
    zip_safe=False,

    install_requires=[
        # TODO for source distributions
        # "cython",

        # bundle, .net, .run and .util
        "sortedcontainers",
        "cloudpickle",
        # "python-snappy",
        "cytoolz",
        "numpy",
        "progressbar2",

        # bundle.run.ssh
        # "asyncssh",  # TODO make optional dependency

        # bundle.cassandra TODO separate?
        "cassandra-driver",
        # "netifaces",
    ],

    extras_require=dict(
        shell=['ipython']
    ),

    ext_modules=extensions,

    entry_points=dict(
        console_scripts=[
            'bndl-shell = bndl.compute.shell:main [shell]',
            'bndl-supervisor = bndl.util.supervisor:main',
        ],
    ),

    test_suite="bndl"
)
