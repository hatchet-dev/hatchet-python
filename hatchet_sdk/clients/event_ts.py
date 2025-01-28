import asyncio
from typing import Any, TypeVar, cast

import grpc.aio


class Event_ts(asyncio.Event):
    """
    Event_ts is a subclass of asyncio.Event that allows for thread-safe setting and clearing of the event.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if self._loop is None:  # type: ignore[has-type]
            self._loop = asyncio.get_event_loop()

    def set(self) -> None:
        if not self._loop.is_closed():
            self._loop.call_soon_threadsafe(super().set)

    def clear(self) -> None:
        self._loop.call_soon_threadsafe(super().clear)


TRequest = TypeVar("TRequest")
TResponse = TypeVar("TResponse")


async def read_with_interrupt(
    listener: grpc.aio.UnaryStreamCall[TRequest, TResponse], interrupt: Event_ts
) -> Any:
    try:
        return cast(Any, await listener.read())
    finally:
        interrupt.set()
