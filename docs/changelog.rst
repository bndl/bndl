Change log
==========

0.3.5
-----
 * Moved hyperloglog and cycloudpickle out of BNDL
 * Default OMP_NUM_THREADS to 2 to ensure a limited number of threads per worker are started
 * Bug in shuffle (introduced in 0.3.3) caused dependency rescheduling to fail
 * Improved shuffle stability and performance
 * Change working directory name pattern / path to avoid issues with multiple users
 * Removed numpy as dependency for setup.py to run
 * Faster file reads when running workers locally without setting location=workers
 * Improved tree_aggregate (and friends)
 * Introduced global configuration object
 * Support pinning workers on cores / NUMA zones
 * Use jemalloc if available in workers
 * Use uvloop on Python 3.5 and up
 * Various bug fixes and tweaks

0.3.4
-----
 * Expose test modules in sdist

0.3.3
-----
 * Optimizations task/partition IO for shuffles
 * Bug in take_sample for tiny datasets
 * Added random distributed numpy arrays
 * Added multivariate summary statistics

0.3.2
-----
 * Initial documentation setup
 * Various bugfixes
