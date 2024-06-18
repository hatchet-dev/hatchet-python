import asyncio
import logging
import random
from functools import wraps
from io import StringIO
from logging import Logger, StreamHandler
from typing import List

from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.rate_limit import RateLimit

from .client import ClientImpl, new_client
from .logger import logger
from .worker import Worker
from .workflow import WorkflowMeta
from .workflows_pb2 import ConcurrencyLimitStrategy, CreateStepRateLimit


class Hatchet:
    client: ClientImpl

    def __init__(
        self,
        debug=False,
        config: ClientConfig = ClientConfig(),
    ):
        # initialize a client
        self.client = new_client(config)

        if not debug:
            logger.disable("hatchet_sdk")

    def concurrency(
        self,
        name: str = "",
        max_runs: int = 1,
        limit_strategy: ConcurrencyLimitStrategy = ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
    ):
        def inner(func):
            func._concurrency_fn_name = name or func.__name__
            func._concurrency_max_runs = max_runs
            func._concurrency_limit_strategy = limit_strategy

            return func

        return inner

    def workflow(
        self,
        name: str = "",
        on_events: list = [],
        on_crons: list = [],
        version: str = "",
        timeout: str = "60m",
        schedule_timeout: str = "5m",
    ):
        def inner(cls):
            cls.on_events = on_events
            cls.on_crons = on_crons
            cls.name = name or str(cls.__name__)
            cls.client = self.client
            cls.version = version
            cls.timeout = timeout
            cls.schedule_timeout = schedule_timeout

            # Define a new class with the same name and bases as the original, but with WorkflowMeta as its metaclass
            return WorkflowMeta(cls.name, cls.__bases__, dict(cls.__dict__))

        return inner

    def step(
        self,
        name: str = "",
        timeout: str = "",
        parents: List[str] = [],
        retries: int = 0,
        rate_limits: List[RateLimit] | None = None,
    ):
        def inner(func):
            limits = None
            if rate_limits:
                limits = [
                    CreateStepRateLimit(key=rate_limit.key, units=rate_limit.units)
                    for rate_limit in rate_limits or []
                ]

            func._step_name = name or func.__name__
            func._step_parents = parents
            func._step_timeout = timeout
            func._step_retries = retries
            func._step_rate_limits = limits
            return func

        return inner

    def on_failure_step(
        self,
        name: str = "",
        timeout: str = "",
        retries: int = 0,
        rate_limits: List[RateLimit] | None = None,
    ):
        def inner(func):
            limits = None
            if rate_limits:
                limits = [
                    CreateStepRateLimit(key=rate_limit.key, units=rate_limit.units)
                    for rate_limit in rate_limits or []
                ]

            func._on_failure_step_name = name or func.__name__
            func._on_failure_step_timeout = timeout
            func._on_failure_step_retries = retries
            func._on_failure_step_rate_limits = limits
            return func

        return inner

    def worker(self, name: str, max_runs: int | None = None):
        return Worker(name=name, max_runs=max_runs, config=self.client.config)
