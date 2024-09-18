import asyncio
import copy
import os
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from typing import Dict, Optional

from loguru import logger

import hatchet_sdk.v2.hatchet as hatchet


def _loopid() -> Optional[int]:
    try:
        return id(asyncio.get_running_loop())
    except:
        return None


_ctxvar: ContextVar[Optional["BackgroundContext"]] = ContextVar(
    "hatchet_background_context", default=None
)


@dataclass
class RunInfo:
    workflow_run_id: Optional[str] = None
    step_run_id: Optional[str] = None

    namespace: str = "<unknown>"
    name: str = "<unknown>"

    # TODO, pid/tid/loopid is not propagated to the engine, we are not able to restore them
    pid: int = os.getpid()
    tid: int = threading.get_ident()
    loopid: Optional[int] = _loopid()

    def copy(self):
        return copy.deepcopy(self)


@dataclass
class BackgroundContext:
    """Background context at function execution time."""

    # The Hatchet client is a required property.
    client: "hatchet.Hatchet"

    current: Optional[RunInfo] = None
    root: Optional[RunInfo] = None
    parent: Optional[RunInfo] = None

    def asdict(self):
        """Return BackgroundContext as a serializable dict."""
        ret = dict()
        if self.current:
            ret["current"] = asdict(self.current)
        if self.root:
            ret["root"] = asdict(self.root)
        if self.parent:
            ret["parent"] = asdict(self.parent)
        return ret

    @staticmethod
    def fromdict(client: "hatchet.Hatchet", data: Dict) -> "BackgroundContext":
        ctx = BackgroundContext(client=client)
        if "current" in data:
            ctx.current = RunInfo(**(data["current"]))
        if "root" in data:
            ctx.root = RunInfo(**(data["root"]))
        if "parent" in data:
            ctx.parent = RunInfo(**(data["parent"]))
        return ctx

    def copy(self):
        ret = BackgroundContext(
            client=self.client,
            current=self.current.copy() if self.current else None,
            parent=self.parent.copy() if self.parent else None,
            root=self.root.copy() if self.root else None,
        )
        return ret

    @staticmethod
    def set(ctx: "BackgroundContext"):
        global _ctxvar
        _ctxvar.set(ctx)

    @staticmethod
    def get() -> Optional["BackgroundContext"]:
        global _ctxvar
        return _ctxvar.get()


def ensure_background_context(
    client: Optional["hatchet.Hatchet"] = None,
) -> BackgroundContext:
    ctx = BackgroundContext.get()
    if ctx is None:
        assert client is not None
        ctx = BackgroundContext(client=client)
        BackgroundContext.set(ctx)
    return ctx


@contextmanager
def WithContext(ctx: BackgroundContext):
    prev = BackgroundContext.get()
    BackgroundContext.set(ctx)
    try:
        logger.trace("using context:\n{}", ctx)
        yield ctx
    finally:
        BackgroundContext.set(prev)


@contextmanager
def WithParentContext(ctx: BackgroundContext):
    """Use the given context as the parent.

    Note that this is to be used in the following pattern:

        with WithParentContext(parent) as ctx:
          ctx.current = ...
          with WithContext(ctx):
            # code in the correct context here

    """
    prev = BackgroundContext.get()

    # NOTE: ctx.current could be None, which means there's no parent.

    child = ctx.copy()
    child.parent = ctx.current.copy() if ctx.current else None
    child.current = None
    BackgroundContext.set(child)
    try:
        yield child
    finally:
        BackgroundContext.set(prev)
