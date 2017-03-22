try:
    from lz4.block import compress as lz4_compress
    from lz4.block import decompress as lz4_decompress
except ImportError:
    from lz4 import compress as lz4_compress
    from lz4 import decompress as lz4_decompress
