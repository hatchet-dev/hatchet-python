import contextvars as cv
from typing import Optional

import grpc
import grpc.aio

import hatchet_sdk.connection as v1
import hatchet_sdk.v2.runtime.context as context

_aio_channel_cv: cv.ContextVar[Optional[grpc.aio.Channel]] = cv.ContextVar(
    "hatchet_background_aio_channel", default=None
)
_channel_cv: cv.ContextVar[Optional[grpc.Channel]] = cv.ContextVar(
    "hatchet_background_channel", default=None
)


def ensure_background_channel() -> grpc.Channel:
    ctx = context.ensure_background_context(client=None)
    channel: Optional[grpc.Channel] = _channel_cv.get()
    if channel is None:
        # TODO: fix the typing of new_conn
        channel = v1.new_conn(ctx.client.config, aio=False)  # type: ignore
        _channel_cv.set(channel)
    assert channel is not None
    return channel


def ensure_background_achannel() -> grpc.aio.Channel:
    ctx = context.ensure_background_context(client=None)
    achannel: Optional[grpc.aio.Channel] = _aio_channel_cv.get()
    if achannel is None:
        # TODO: fix the typing of new_conn
        achannel = v1.new_conn(ctx.client.config, aio=True)  # type: ignore
        _aio_channel_cv.set(achannel)
    assert achannel is not None
    return achannel


def reset_background_channel():
    c = _channel_cv.get()
    if c is not None:
        c.close()
    _channel_cv.set(None)


async def reset_background_achannel():
    c: Optional[grpc.aio.Channel] = _aio_channel_cv.get()
    if c is not None:
        await c.close()
    _aio_channel_cv.set(None)
