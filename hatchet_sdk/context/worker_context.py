class WorkerContext:
    _worker_id: str
    _registered_workflow_names: list[str] = []

    def __init__(self, worker_id: str, _registered_workflows: dict):
        self._worker_id = worker_id
        self._registered_workflow_names = _registered_workflows

    def id(self):
        return self._worker_id

    def has_workflow(self, workflow_name: str):
        return workflow_name in self._registered_workflow_names
