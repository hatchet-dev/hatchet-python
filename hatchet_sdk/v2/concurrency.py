from typing import Callable

from hatchet_sdk.context import Context
from hatchet_sdk.contracts.workflows_pb2 import ConcurrencyLimitStrategy


class ConcurrencyFunction:
    def __init__(
        self,
        func: Callable[[Context], str],
        name: str = "concurrency",
        max_runs: int = 1,
        limit_strategy: ConcurrencyLimitStrategy = ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
    ):
        self.func = func
        self.name = name
        self.max_runs = max_runs
        self.limit_strategy = limit_strategy
        self.namespace = "default"

    def set_namespace(self, namespace: str):
        self.namespace = namespace

    def get_action_name(self) -> str:
        return self.namespace + ":" + self.name

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __str__(self):
        return f"{self.name}({self.max_runs})"

    def __repr__(self):
        return f"{self.name}({self.max_runs})"


def concurrency(
    name: str = "",
    max_runs: int = 1,
    limit_strategy: ConcurrencyLimitStrategy = ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
):
    def inner(func: Callable[[Context], str]) -> ConcurrencyFunction:
        return ConcurrencyFunction(func, name, max_runs, limit_strategy)

    return inner
