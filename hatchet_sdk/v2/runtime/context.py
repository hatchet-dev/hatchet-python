import asyncio
import copy
import os
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, asdict
from typing import Optional, Dict

import hatchet_sdk.v2.hatchet as hatchet

from loguru import logger


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
    def fromdict(d: Dict) -> "BackgroundContext":
        ctx = BackgroundContext()
        if "current" in d:
            ctx.current = RunInfo(**(d["current"]))
        if "root" in d:
            ctx.root = RunInfo(**(d["root"]))
        if "parent" in d:
            ctx.parent = RunInfo(**(d["parent"]))
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


@contextmanager
def EnsureContext(client: Optional["hatchet.Hatchet"] = None):
    cleanup = False
    ctx = BackgroundContext.get()
    if ctx is None:
        cleanup = True
        assert client is not None
        ctx = BackgroundContext(client=client)
        BackgroundContext.set(ctx)
    try:
        logger.trace("using context:\n{}", ctx)
        yield ctx
    finally:
        if cleanup:
            BackgroundContext.set(None)


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
    prev = BackgroundContext.get()

    child = ctx.copy()
    child.parent = ctx.current.copy()
    child.current = None
    BackgroundContext.set(child)
    try:
        logger.trace("using context:\n{}", child)
        yield child
    finally:
        BackgroundContext.set(prev)
