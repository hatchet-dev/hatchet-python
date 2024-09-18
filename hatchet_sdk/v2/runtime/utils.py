from collections.abc import AsyncGenerator
import asyncio

from typing import TypeVar

from contextlib import suppress

T = TypeVar("T")
I = TypeVar("I")


async def InterruptableAgen(
    agen: AsyncGenerator[T],
    interrupt: asyncio.Queue[I],
    timeout: float,
) -> AsyncGenerator[T | I]:

    queue: asyncio.Queue[T | StopAsyncIteration] = asyncio.Queue()

    async def producer():
        async for item in agen:
            await queue.put(item)
        await queue.put(StopAsyncIteration())

    producer_task = asyncio.create_task(producer())

    while True:
        with suppress(asyncio.TimeoutError):
            item = await asyncio.wait_for(queue.get(), timeout=timeout)
            # it is not timeout if we reach this line
            if isinstance(item, StopAsyncIteration):
                break
            else:
                yield item

        with suppress(asyncio.QueueEmpty):
            v = interrupt.get_nowait()
            # we are interrupted if we reach this line
            yield v
            break

    producer_task.cancel()
    with suppress(asyncio.CancelledError):
        await producer_task
