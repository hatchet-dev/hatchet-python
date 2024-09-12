import asyncio
import copy
import os
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional

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
class _RunInfo:
    workflow_run_id: Optional[str] = None
    step_run_id: Optional[str] = None

    namespace: str = "<unknown>"
    name: str = "<unknown>"

    pid: int = os.getpid()
    tid: int = threading.get_ident()
    loopid: Optional[int] = _loopid()

    def copy(self):
        return copy.deepcopy(self)


@dataclass
class BackgroundContext:
    """Background context at function execution time."""

    current: _RunInfo = _RunInfo()
    parent: Optional[_RunInfo] = None
    root: _RunInfo = current

    # The Hatchet client is a required property.
    client: hatchet.Hatchet

    def set_workflow_run_id(self, id: str):
        self.current.workflow_run_id = id

    def set_step_run_id(self, id: str):
        self.current.step_run_id = id

    def copy(self):
        ret = BackgroundContext(
            client=self.client,
            current=self.current.copy(),
            parent=self.parent.copy() if self.parent else None,
            root=self.root.copy(),
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
def EnsureContext(client: Optional[hatchet.Hatchet] = None):
    cleanup = False
    ctx = BackgroundContext.get()
    if ctx is None:
        cleanup = True
        assert client is not None
        ctx = BackgroundContext(client=client)
        BackgroundContext.set(ctx)
    try:
        yield ctx
    finally:
        if cleanup:
            BackgroundContext.set(None)


@contextmanager
def WithContext(ctx: BackgroundContext):
    prev = BackgroundContext.get()
    BackgroundContext.set(ctx)
    try:
        yield ctx
    finally:
        BackgroundContext.set(prev)


@contextmanager
def EnterFunc():
    with EnsureContext() as current:
        child = current.copy()
        child.parent = current.current.copy()
        child.current = _RunInfo()
        with WithContext(child) as ctx:
            try:
                yield ctx
            finally:
                pass
