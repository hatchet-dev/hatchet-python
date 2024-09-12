import asyncio

import dotenv
import pytest

from hatchet_sdk.v2.hatchet import Hatchet
from hatchet_sdk.v2.runtime.worker import WorkerOptions
import logging

dotenv.load_dotenv()

hatchet = Hatchet(debug=True)

logging.getLogger("asyncio").setLevel(logging.DEBUG)


@hatchet.function()
def foo():
    pass


@pytest.mark.asyncio
async def test_worker():
    worker = hatchet.worker(WorkerOptions(name="worker", actions=["default:foo"]))
    await worker.start()
    foo()
    await asyncio.sleep(10)
    await worker.shutdown()
