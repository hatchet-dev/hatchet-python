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
    print("entering Foo")
    print("result from bar: ", bar("from foo"))
    return "foo"


@hatchet.function()
def bar(x):
    print("entering Bar")
    print("arguments for bar: ", x)
    return "bar"


@pytest.mark.asyncio
async def test_worker():
    worker = hatchet.worker(
        WorkerOptions(name="worker", actions=["default:foo", "default:bar"])
    )
    await worker.start()
    print("result from foo: ", foo())
    await asyncio.sleep(10)
    await worker.shutdown()
    return None
