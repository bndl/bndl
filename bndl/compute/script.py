from bndl.compute.driver import main, argparser
from bndl.util.supervisor import  split_args
import copy


__all__ = ['ctx']


argparser = copy.copy(argparser)
argparser.prog = 'bndl.compute.script'
args = argparser.parse_args(split_args())
ctx = main(args, daemon=True)
