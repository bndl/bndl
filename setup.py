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


ext = re.compile(r'\.pyx$')

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
    name='bndl',
    version='0.0.1',
    url='https://stash.tgho.nl/projects/THCLUSTER/repos/bndl/browse',
    description='Bundle compute resources with BNDL',
    author='Frens Jan Rumph',
    author_email='mail@frensjan.nl',

    packages=(
        find_packages()
    ),

    include_package_data=True,
    zip_safe=False,

    setup_requires=[
        'cython'
    ],

    install_requires=[
        'sortedcontainers',
        'cloudpickle',
        'cytoolz',
        'numpy',
        'flask',
        'mmh3',

        'cassandra-driver',
        'lz4',
        'scales',

        'elasticsearch',
        'netifaces',

        'scipy',
        'scikit-learn',
    ],

    extras_require=dict(
        shell=['ipython'],
        dev=[
            'cython',
            'pytest',
            'pytest-cov',
            'pylint',
            'flake8',
            'sphinx',
            'sphinx-autobuild',
        ],
    ),

    ext_modules=extensions,

    entry_points=dict(
        console_scripts=[
            'bndl-shell = bndl.compute.shell:main [shell]',
            'bndl-supervisor = bndl.util.supervisor:main',
        ],
    ),

    test_suite='bndl'
)
