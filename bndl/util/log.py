import logging


TRACE = 5

def install_trace_logging():
    logging.TRACE = TRACE
    logging.addLevelName(TRACE, 'TRACE')

    def trace(self, message, *args, **kws):
        if self.isEnabledFor(TRACE):
            self._log(TRACE, message, args, **kws)

    logging.Logger.trace = trace
