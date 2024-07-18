class WorkerContext():
    _worker_id: str

    def __init__(self, worker_id: str):
        self._worker_id = worker_id

    def id(self):
        return self._worker_id
