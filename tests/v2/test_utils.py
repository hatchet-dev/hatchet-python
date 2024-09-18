import asyncio
import logging


import pytest
from loguru import logger

from hatchet_sdk.v2.runtime.utils import interuptable


logging.getLogger("asyncio").setLevel(logging.DEBUG)


async def producer():
    for i in range(10):
        await asyncio.sleep(0.5)
        logger.info("yielding {}", i)
        yield i


async def consumer(agen):
    async for item in agen:
        logger.info("consuming: {}", item)


@pytest.mark.asyncio
async def test_interruptable_agen():

    q = asyncio.Queue()
    agen = interuptable(producer(), q, 1)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(consumer(agen))
        await asyncio.sleep(2)
        await q.put({})
