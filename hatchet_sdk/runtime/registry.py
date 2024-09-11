from typing import Dict, List

import hatchet_sdk.v2.callable as v2


class ActionRegistry:
    """A registry from action names (e.g. 'namespace:func') to Hatchet's callables.

    This is intended to be used per Hatchet client instance.
    """

    registry: Dict[str, v2.HatchetCallableBase] = dict()

    def register(self, callable: v2.HatchetCallableBase) -> str:
        key = "{namespace}:{name}".format(
            namespace=callable._.namespace, name=callable._.name
        )
        self.registry[key] = callable
        return key
