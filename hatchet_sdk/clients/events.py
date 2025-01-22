import asyncio
import datetime
import json
from typing import Any, Dict, List, Optional, TypedDict, cast
from uuid import uuid4

import grpc
from google.protobuf import timestamp_pb2
from pydantic import BaseModel, Field

from hatchet_sdk.clients.rest.tenacity_utils import tenacity_retry
from hatchet_sdk.contracts.events_pb2 import (
    BulkPushEventRequest,
    Event,
    PushEventRequest,
    PutLogRequest,
    PutStreamEventRequest,
)
from hatchet_sdk.contracts.events_pb2_grpc import EventsServiceStub
from hatchet_sdk.utils.serialization import flatten
from hatchet_sdk.utils.tracing import (
    OTEL_CARRIER_KEY,
    create_carrier,
    create_tracer,
    inject_carrier_into_metadata,
    parse_carrier_from_metadata,
)
from hatchet_sdk.utils.types import JSONSerializableDict

from ..loader import ClientConfig
from ..metadata import get_metadata


def new_event(conn: grpc.Channel, config: ClientConfig) -> "EventClient":
    return EventClient(
        client=EventsServiceStub(conn),  # type: ignore[no-untyped-call]
        config=config,
    )


def proto_timestamp_now() -> timestamp_pb2.Timestamp:
    t = datetime.datetime.now().timestamp()
    seconds = int(t)
    nanos = int(t % 1 * 1e9)

    return timestamp_pb2.Timestamp(seconds=seconds, nanos=nanos)


class PushEventOptions(BaseModel):
    additional_metadata: JSONSerializableDict = Field(default_factory=dict)
    namespace: str | None = None


class BulkPushEventOptions(BaseModel):
    namespace: str | None = None
    otel_carrier: dict[str, str] = Field(default_factory=dict)


class BulkPushEventWithMetadata(BaseModel):
    key: str
    payload: Any
    additional_metadata: JSONSerializableDict = Field(default_factory=dict)


class EventClient:
    def __init__(self, client: EventsServiceStub, config: ClientConfig):
        self.client = client
        self.token = config.token
        self.namespace = config.namespace
        self.otel_tracer = create_tracer(config=config)

    async def async_push(
        self,
        event_key: str,
        payload: dict[str, Any],
        options: PushEventOptions = PushEventOptions(),
    ) -> Event:
        return await asyncio.to_thread(
            self.push, event_key=event_key, payload=payload, options=options
        )

    async def async_bulk_push(
        self,
        events: list[BulkPushEventWithMetadata],
        options: BulkPushEventOptions = BulkPushEventOptions(),
    ) -> List[Event]:
        return await asyncio.to_thread(self.bulk_push, events=events, options=options)

    @tenacity_retry
    def push(
        self,
        event_key: str,
        payload: dict[str, Any],
        options: PushEventOptions = PushEventOptions(),
    ) -> Event:
        ctx = parse_carrier_from_metadata(options.additional_metadata)

        with self.otel_tracer.start_as_current_span(
            "hatchet.push", context=ctx
        ) as span:
            carrier = create_carrier()
            namespace = options.namespace or self.namespace

            namespaced_event_key = namespace + event_key

            try:
                meta = inject_carrier_into_metadata(
                    options.additional_metadata,
                    carrier,
                )
                meta_bytes = None if meta is None else json.dumps(meta)
            except Exception as e:
                raise ValueError(f"Error encoding meta: {e}")

            span.set_attributes(flatten(meta, parent_key="", separator="."))

            try:
                payload_str = json.dumps(payload)
            except (TypeError, ValueError) as e:
                raise ValueError(f"Error encoding payload: {e}")

            request = PushEventRequest(
                key=namespaced_event_key,
                payload=payload_str,
                eventTimestamp=proto_timestamp_now(),
                additionalMetadata=meta_bytes,
            )

            span.add_event("Pushing event", attributes={"key": namespaced_event_key})

            return cast(
                Event, self.client.Push(request, metadata=get_metadata(self.token))
            )

    @tenacity_retry
    def bulk_push(
        self,
        events: List[BulkPushEventWithMetadata],
        options: BulkPushEventOptions,
    ) -> List[Event]:
        namespace = options.namespace or self.namespace
        bulk_push_correlation_id = uuid4()

        ctx = parse_carrier_from_metadata({OTEL_CARRIER_KEY: options.otel_carrier})

        bulk_events = []
        for event in events:
            with self.otel_tracer.start_as_current_span(
                "hatchet.bulk_push", context=ctx
            ) as span:
                carrier = create_carrier()
                span.set_attribute(
                    "bulk_push_correlation_id", str(bulk_push_correlation_id)
                )

                event_key = namespace + event.key
                payload = event.payload

                meta = inject_carrier_into_metadata(event.additional_metadata, carrier)
                span.set_attributes(flatten(meta, parent_key="", separator="."))

                try:
                    meta_str = json.dumps(meta)
                except Exception as e:
                    raise ValueError(f"Error encoding meta: {e}")

                try:
                    payload = json.dumps(payload)
                except (TypeError, ValueError) as e:
                    raise ValueError(f"Error encoding payload: {e}")

                request = PushEventRequest(
                    key=event_key,
                    payload=payload,
                    eventTimestamp=proto_timestamp_now(),
                    additionalMetadata=meta_str,
                )
                bulk_events.append(request)

        bulk_request = BulkPushEventRequest(events=bulk_events)

        span.add_event("Pushing bulk events")
        response = self.client.BulkPush(bulk_request, metadata=get_metadata(self.token))

        return cast(
            list[Event],
            response.events,
        )

    def log(self, message: str, step_run_id: str) -> None:
        try:
            request = PutLogRequest(
                stepRunId=step_run_id,
                createdAt=proto_timestamp_now(),
                message=message,
            )

            self.client.PutLog(request, metadata=get_metadata(self.token))
        except Exception as e:
            raise ValueError(f"Error logging: {e}")

    def stream(self, data: str | bytes, step_run_id: str) -> None:
        try:
            if isinstance(data, str):
                data_bytes = data.encode("utf-8")
            elif isinstance(data, bytes):
                data_bytes = data
            else:
                raise ValueError("Invalid data type. Expected str, bytes, or file.")

            request = PutStreamEventRequest(
                stepRunId=step_run_id,
                createdAt=proto_timestamp_now(),
                message=data_bytes,
            )
            self.client.PutStreamEvent(request, metadata=get_metadata(self.token))
        except Exception as e:
            raise ValueError(f"Error putting stream event: {e}")
