from collections.abc import AsyncGenerator, Callable
import asyncio
import tenacity
import grpc
import multiprocessing as mp
import multiprocessing.queues as mpq
from typing import TypeVar, Tuple


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


async def ForeverAgen(
    agen_factory: Callable[[], AsyncGenerator[T]], exceptions: Tuple[Exception]
) -> AsyncGenerator[T | Exception]:
    """Run a async generator forever until its cancelled.

    Args:
        agen_factory: a callable that returns the async generator of type T
        exceptions: a tuple of exceptions that should be suppressed and yielded.
            Exceptions not listed here will be re-raised.

    Returns:
        An async generator that yields T or yields the suppressed exceptions.
    """
    while True:
        agen = agen_factory()
        try:
            async for item in agen:
                yield item
        except Exception as e:
            if isinstance(e, exceptions):
                yield e
            else:
                raise


async def QueueAgen(inbound: asyncio.Queue[T] | mpq.Queue[T]) -> AsyncGenerator[T]:
    while True:
        item = await asyncio.to_thread(inbound.get)
        yield item
