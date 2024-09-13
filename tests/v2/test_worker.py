import asyncio
import logging
import sys

import dotenv
import pytest
from loguru import logger

from hatchet_sdk.v2.hatchet import Hatchet
from hatchet_sdk.v2.runtime.worker import WorkerOptions

logger.remove()
logger.add(sys.stdout, level="TRACE")

dotenv.load_dotenv()

hatchet = Hatchet(debug=True)

logging.getLogger("asyncio").setLevel(logging.DEBUG)


@hatchet.function()
def foo():
    print("HAHAHA")
    pass


@pytest.mark.asyncio
async def test_worker():
    worker = hatchet.worker(WorkerOptions(name="worker", actions=["default:foo"]))
    await worker.start()
    hatchet._runner.start()
    foo()
    await asyncio.sleep(10)
    await worker.shutdown()
    await hatchet._runner.shutdown()
