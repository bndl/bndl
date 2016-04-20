def try_pyximport_install(*args, **kwargs):
	try:
		import pyximport
		pyximport.install(*args, **kwargs)
	except ImportError:
		pass