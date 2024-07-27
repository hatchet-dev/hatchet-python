from hatchet_sdk.loader import ClientConfig

from ..client import new_client_raw


class WorkerContext:
    _worker_id: str
    _registered_workflow_names: list[str] = []
    _labels: dict[str, str | int] = {}

    def __init__(self, labels: dict[str, str | int], config: ClientConfig = {}):
        self._labels = labels
        self.client = new_client_raw(config)

    def labels(self):
        return self._labels

    def upsert_labels(self, labels: dict[str, str | int]):
        return self.client.dispatcher.upsert_worker_labels(self._worker_id, labels)

    def id(self):
        return self._worker_id

    def has_workflow(self, workflow_name: str):
        return workflow_name in self._registered_workflow_names
