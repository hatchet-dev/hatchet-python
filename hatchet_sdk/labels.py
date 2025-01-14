from pydantic import BaseModel

from hatchet_sdk.contracts.workflows_pb2 import WorkerLabelComparator


class DesiredWorkerLabel(BaseModel):
    value: str | int
    required: bool = False
    weight: int | None = None
    comparator: WorkerLabelComparator | None = None
