import asyncio
import logging
import sys

import dotenv
import pytest
from loguru import logger

from hatchet_sdk.v2.hatchet import Hatchet
from hatchet_sdk.v2.runtime.listeners import WorkflowRunEventListener

logger.remove()
logger.add(sys.stdout, level="TRACE")

dotenv.load_dotenv()

hatchet = Hatchet(debug=True)

logging.getLogger("asyncio").setLevel(logging.DEBUG)


async def interrupt(listener):
    await asyncio.sleep(2)
    logger.trace("interupt")
    await listener._interrupt()
    logger.trace("interrupted")


@pytest.mark.asyncio
async def test_listener_shutdown():
    listener = WorkflowRunEventListener()
    task = asyncio.create_task(listener.loop())
    task2 = asyncio.create_task(interrupt(listener))
    sub = await listener.subscribe("bar-vj13ex/bar")
    await sub.future
    await task
