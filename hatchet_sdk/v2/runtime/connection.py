import hatchet_sdk.connection as v1
import contextvars as cv
import grpc
import grpc.aio
from typing import Optional

import hatchet_sdk.v2.runtime.context as context


_aio_channel_cv: cv.ContextVar[Optional[grpc.aio.Channel]] = cv.ContextVar(
    "hatchet_background_aio_channel", default=None
)
_channel_cv: cv.ContextVar[Optional[grpc.Channel]] = cv.ContextVar(
    "hatchet_background_channel", default=None
)


def ensure_background_channel() -> grpc.Channel:
    ctx = context.ensure_background_context(client=None)
    channel: grpc.Channel = _channel_cv.get()
    if channel is None:
        channel = v1.new_conn(ctx.client.config, aio=False)
        _channel_cv.set(channel)
    return channel


def ensure_background_achannel() -> grpc.aio.Channel:
    ctx = context.ensure_background_context(client=None)
    achannel: grpc.aio.Channel = _aio_channel_cv.get()
    if achannel is None:
        achannel = v1.new_conn(ctx.client.config, aio=True)
        _aio_channel_cv.set(achannel)
    return achannel


def reset_background_channel():
    c = _channel_cv.get()
    if c is not None:
        c.close()
    _channel_cv.set(None)


async def reset_background_achannel():
    c: grpc.aio.Channel = _aio_channel_cv.get()
    if c is not None:
        await c.close()
    _aio_channel_cv.set(None)
