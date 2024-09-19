import asyncio
import os
import threading

import hatchet_sdk.logger as v1


def _loopid():
    try:
        return id(asyncio.get_running_loop())
    except:
        return -1


class HatchetLogger:
    def log(self, *args, **kwargs):
        v1.logger.log(*args, **kwargs)

    def debug(self, *args, **kwargs):
        v1.logger.debug(*args, **kwargs)

    def info(self, *args, **kwargs):
        pid = str(os.getpid())
        tid = str(threading.get_ident())
        loopid = str(_loopid())
        v1.logger.info(f"{pid}, {tid}, {loopid}")
        v1.logger.info(*args, **kwargs)


logger = HatchetLogger()
