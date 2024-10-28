import hashlib
from typing import Annotated, Optional

from pydantic import BaseModel, Field, StrictStr


class Compute(BaseModel):
    pool: Optional[str] = Field(
        default="default",
        description="The name of the compute pool to use",
    )
    num_replicas: Annotated[int, Field(le=1000, strict=True, ge=0)] = Field(
        default=1, alias="num_replicas"
    )
    # TODO: change to ManagedWorkerRegion
    regions: Optional[list[str]] = Field(
        default=None, description="The regions to deploy the worker to"
    )
    cpu_kind: StrictStr = Field(
        description="The kind of CPU to use for the worker", alias="cpu_kind"
    )
    cpus: Annotated[int, Field(le=64, strict=True, ge=1)] = Field(
        description="The number of CPUs to use for the worker"
    )
    memory_mb: Annotated[int, Field(le=65536, strict=True, ge=1024)] = Field(
        description="The amount of memory in MB to use for the worker",
        alias="memory_mb",
    )

    def __json__(self):
        return self.model_dump_json()

    def hash(self):
        return hashlib.sha256(self.__json__().encode()).hexdigest()
