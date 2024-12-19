from typing import TypedDict


class DesiredWorkerLabel(TypedDict, total=False):
    value: str | int
    required: bool | None
    weight: int | None
    # _ClassVar[WorkerLabelComparator] TODO figure out type
    comparator: int | None
