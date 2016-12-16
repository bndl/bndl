Change log
==========

0.3.5
-----
 * Moved hyperloglog and cycloudpickle out of BNDL
 * Default OMP_NUM_THREADS to 2 to ensure a limited number of threads per worker are started
 * Bug in shuffle (introduced in 0.3.3) caused dependency rescheduling to fail

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
