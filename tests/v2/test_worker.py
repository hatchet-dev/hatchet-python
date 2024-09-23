import time
import asyncio
import logging
import sys
import multiprocessing as mp
import dotenv
import pytest
from loguru import logger

from hatchet_sdk.v2.hatchet import Hatchet
from hatchet_sdk.v2.runtime.worker import WorkerOptions, WorkerProcess
from concurrent.futures import ThreadPoolExecutor

# logger.remove()
# logger.add(
#     sys.stdout, level="TRACE"
# )  # , format="{level}\t|{module}:{function}:{line}[{process}:{thread}] - {message}")

dotenv.load_dotenv()

hatchet = Hatchet(debug=True)

logging.getLogger("asyncio").setLevel(logging.DEBUG)


@hatchet.function()
def foo():
    print("entering Foo")
    print("result from bar: ", bar("from foo").result())
    return "foo"


@hatchet.function()
def bar(x):
    print("entering Bar")
    print("arguments for bar: ", x)
    return "bar"


@pytest.mark.asyncio
async def test_worker():

    worker = hatchet.worker(
        options=WorkerOptions(name="worker", actions=["default:foo", "default:bar"])
    )
    await worker.start()
    print("result from foo: ", await asyncio.to_thread(foo().result))
    await asyncio.sleep(10)
    await worker.shutdown()
    return None


# def test_worker_process():
#     to_worker = mp.Queue()
#     from_worker = mp.Queue()
#     p = WorkerProcess(
#         config=hatchet.config,
#         options=WorkerOptions(name="worker", actions=[]),
#         inbound=to_worker,
#         outbound=from_worker,
#     )

#     pool = ThreadPoolExecutor()
#     id = pool.submit(from_worker.get)
#     print(p.start())
#     print(id.result())
#     time.sleep(10)
#     print("shutting down")
#     p.shutdown()

#     to_worker.close()
#     from_worker.close()
