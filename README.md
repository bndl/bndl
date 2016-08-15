BNDL
====

Bundle compute resources in Python across cores and machines.

Install
------------

BNDL can be installed through pip:

```python
pip install bndl
```

Install dependencies with:

```python
pip install bndl[dev]
```


Usage
-----

### Shell

The main commands to use BNDL are:

 * `bndl-compute-shell` and
 * `bndl-compute-workers`


### Script
Python scripts use `bndl.compute.run`, e.g.:

```python
from bndl.compute.run import ctx
print(ctx.range(1000).map(str).map(len).stats())
```
