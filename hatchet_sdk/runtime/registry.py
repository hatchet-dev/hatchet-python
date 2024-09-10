from typing import Dict, List


class ActionRegistry:

    _registry: Dict[str, "HatchetCallable"] = dict()

    def register(self, callable: "HatchetCallable") -> str:
        key = "{namespace}:{name}".format(
            namespace=callable._.namespace, name=callable._.name
        )
        self._registry[key] = callable
        return key

    def list(self) -> List[str]:
        return list(self._registry.keys())


global_registry = ActionRegistry()
