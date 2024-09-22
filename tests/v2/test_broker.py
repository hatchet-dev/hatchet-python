import asyncio
import logging
import sys
import queue
import threading

from concurrent.futures import ThreadPoolExecutor

# import dotenv
import pytest
from loguru import logger

# from hatchet_sdk.v2.hatchet import Hatchet
from hatchet_sdk.v2.runtime.broker import QueueToFutureBroker

logger.remove()
logger.add(sys.stdout, level="TRACE")

# dotenv.load_dotenv()

# hatchet = Hatchet(debug=True)

logging.getLogger("asyncio").setLevel(logging.DEBUG)


to_broker = queue.Queue()
to_server = queue.Queue()
exec = ThreadPoolExecutor()
broker = QueueToFutureBroker(
    inbound=to_broker,
    outbound=to_server,
    req_key=lambda x: x,
    resp_key=lambda x: x,
    executor=exec,
)


def echo(p: queue.Queue, q: queue.Queue):
    while True:
        item = p.get()
        logger.trace("echo {}", item)
        q.put(item)


echo_f = exec.submit(echo, to_server, to_broker)


# def test_broker():
#     fut = exec.submit(asyncio.run, broker.loop())
#     f = broker.submit(1)
#     print(f.result())
#     fut.cancel()


@pytest.mark.asyncio
async def test_broker_async():
    task = asyncio.create_task(broker.loop())
    f = await broker.asubmit(2)
    print(await f)
    task.cancel()

