BNDL
====

Bundle compute resources in Python.

Dependencies
------------

Using BNDL requires:
 * sortedcontainers
 * cloudpickle
 * cytoolz
 * numpy
 * flask
 * cassandra-driver
 
These dependencies are installed when you install BNDL.
 
Developing BNDL requires:
 * cython
 * pytest
 * pytest-cov
 * pylint
 * flake8
 * sphinx
 * sphinx-autobuild
 
The development dependencies are installed when installing the dev dependencies, e.g. with:

```python
pip install bndl[dev]
```


Usage
-----

### Shell

The main commands to use BNDL are:

 * `bndl-shell` and
 * `bndl-supervisor`


### Script
Python scripts use `bndl.compute.script`, e.g.:

```python
from bndl.compute.script import ctx
print(ctx.range(1000).map(str).map(len).stats())
```
