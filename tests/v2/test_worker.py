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
    print("Foo")
    bar("from foo")
    return "foo"


@hatchet.function()
def bar(x):
    print(x)
    return "bar"


@pytest.mark.asyncio
async def test_worker():
    worker = hatchet.worker(WorkerOptions(name="worker", actions=["default:foo", "default:bar"]))
    await worker.start()
    foo()
    await asyncio.sleep(10)
    await worker.shutdown()
    return None
