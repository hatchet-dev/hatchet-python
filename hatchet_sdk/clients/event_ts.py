import asyncio
from typing import Any


class Event_ts(asyncio.Event):
    """
    Event_ts is a subclass of asyncio.Event that allows for thread-safe setting and clearing of the event.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._loop: asyncio.AbstractEventLoop

        if self._loop is None:
            self._loop = asyncio.get_event_loop()

    def set(self) -> None:
        if not self._loop.is_closed():
            self._loop.call_soon_threadsafe(super().set)

    def clear(self) -> None:
        self._loop.call_soon_threadsafe(super().clear)


async def read_with_interrupt(listener: Any, interrupt: Event_ts) -> Any:
    try:
        result = await listener.read()
        return result
    finally:
        interrupt.set()
