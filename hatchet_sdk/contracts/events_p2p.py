# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.2.7](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.4 
# Pydantic Version: 2.8.2 
from datetime import datetime
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import Field
import typing


class Event(BaseModel):
# the tenant id
    tenantId: str = Field(default="")
# the id of the event
    eventId: str = Field(default="")
# the key for the event
    key: str = Field(default="")
# the payload for the event
    payload: str = Field(default="")
# when the event was generated
    eventTimestamp: datetime = Field(default_factory=datetime.now)
# the payload for the event
    additionalMetadata: typing.Optional[str] = Field(default="")

class PutLogRequest(BaseModel):
# the step run id for the request
    stepRunId: str = Field(default="")
# when the log line was created
    createdAt: datetime = Field(default_factory=datetime.now)
# the log line message
    message: str = Field(default="")
# the log line level
    level: typing.Optional[str] = Field(default="")
# associated log line metadata
    metadata: str = Field(default="")

class PutLogResponse(BaseModel):    pass

class PutStreamEventRequest(BaseModel):
# the step run id for the request
    stepRunId: str = Field(default="")
# when the stream event was created
    createdAt: datetime = Field(default_factory=datetime.now)
# the stream event message
    message: bytes = Field(default=b"")
# associated stream event metadata
    metadata: str = Field(default="")

class PutStreamEventResponse(BaseModel):    pass

class PushEventRequest(BaseModel):
# the key for the event
    key: str = Field(default="")
# the payload for the event
    payload: str = Field(default="")
# when the event was generated
    eventTimestamp: datetime = Field(default_factory=datetime.now)
# metadata for the event
    additionalMetadata: typing.Optional[str] = Field(default="")

class ReplayEventRequest(BaseModel):
# the event id to replay
    eventId: str = Field(default="")
