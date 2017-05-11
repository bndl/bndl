#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import re
import sys

from setuptools import setup, find_packages, Extension
import pkg_resources

import bndl


install_requires = [
    'sortedcontainers',
    'cycloudpickle',
    'cyhll',
    'cytoolz',
    'numpy',
    'scipy',
    'pandas',
    'flask',
    'mmh3',
    'psutil>=4.4',
    'tblib',
    'marisa_trie',
    'yappi<=0.93',
    'lz4',
    'cyheapq',
]

if sys.version_info < (3, 5):
    install_requires.append('scandir')

if sys.version_info >= (3, 5):
    install_requires.append('uvloop')


dev_requires = [
    'cython>=0.25.2',
    'pytest',
    'pytest-cov',
    'pylint',
    'flake8',
    'sphinx',
    'sphinx-autobuild',
    'sphinx-rtd-theme',
    'sphinxcontrib-programoutput',
    'pandas',
    'msgpack-python',
]


def get_distributions(requirements):
    packages = set(p.name for p in pkg_resources.parse_requirements(requirements))
    for p in packages:
        yield pkg_resources.get_distribution(p)


def print_distributions(requirements):
    dists = sorted(get_distributions(requirements), key=lambda d: d.project_name)
    for dist in dists:
        print(dist.project_name.ljust(30), dist.version)


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


try:
    from Cython.Build import cythonize
    extensions = cythonize(extensions, compiler_directives={
        'language_level': 3
    })
except ImportError:
    pass


if __name__ == '__main__':
    setup(
        name='bndl',
        version=bndl.__version__,
        url='https://stash.tgho.nl/projects/THCLUSTER/repos/bndl/browse',
        description='Bundle compute resources with BNDL',
        long_description=open('README.rst').read(),
        author='Frens Jan Rumph',
        author_email='mail@frensjan.nl',

        packages=(
            find_packages()
        ),

        data_files=[
            (
                os.path.join('docs/bndl', root.replace('docs/build/', '')),
                [os.path.join(root, f) for f in files]
            )
            for root, dirs, files in os.walk('docs/build/html')
        ],

        include_package_data=True,
        zip_safe=False,

        install_requires=install_requires,
        extras_require=dict(
            dev=dev_requires,
        ),

        ext_modules=extensions,

        entry_points=dict(
            console_scripts=[
                'bndl-compute-shell = bndl.compute.shell:main',
                'bndl-compute-worker = bndl.compute.worker:main',
            ],
        ),

        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
        ],
    )
