from hatchet_sdk.clients.dispatcher.dispatcher import DispatcherClient


class WorkerContext:
    _worker_id: str = None
    _registered_workflow_names: list[str] = []
    _labels: dict[str, str | int] = {}

    def __init__(self, labels: dict[str, str | int], client: DispatcherClient):
        self._labels = labels
        self.client = client

    def labels(self):
        return self._labels

    def upsert_labels(self, labels: dict[str, str | int]):
        self.client.upsert_worker_labels(self._worker_id, labels)
        self._labels.update(labels)

    async def async_upsert_labels(self, labels: dict[str, str | int]):
        await self.client.async_upsert_worker_labels(self._worker_id, labels)
        self._labels.update(labels)

    def id(self):
        return self._worker_id

    # def has_workflow(self, workflow_name: str):
    #     return workflow_name in self._registered_workflow_names
