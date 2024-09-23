import sys
from typing import Dict

from loguru import logger

import hatchet_sdk.v2.callable as callable
import hatchet_sdk.v2.hatchet as hatchet


class ActionRegistry:
    """A registry from action names (e.g. 'namespace:func') to Hatchet's callables.

    This is intended to be used per Hatchet client instance.
    """

    def __init__(self):
        self.registry: Dict[str, "callable.HatchetCallableBase"] = dict()

    def add(self, key: str, callable: "callable.HatchetCallableBase"):
        if key in self.registry:
            raise KeyError(f"duplicated Hatchet callable: {key}")
        self.registry[key] = callable

    def register_all(self, client: "hatchet.Hatchet"):
        for callable in self.registry.values():
            proto = callable._to_workflow_proto()
            try:
                client.admin.put_workflow(proto.name, proto)
            except Exception as e:
                logger.error("failed to register workflow: {}", proto.name)
                logger.exception(e)
                sys.exit(1)
