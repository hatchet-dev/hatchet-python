from typing import Any, Dict, List

from pydantic import StrictInt, StrictStr

from hatchet_sdk.clients.rest.models.event_list import EventList
from hatchet_sdk.clients.rest.models.event_order_by_direction import (
    EventOrderByDirection,
)
from hatchet_sdk.clients.rest.models.event_order_by_field import EventOrderByField
from hatchet_sdk.clients.rest.models.replay_event_request import ReplayEventRequest
from hatchet_sdk.clients.rest.models.workflow_run_status import WorkflowRunStatus
from hatchet_sdk.clients.rest.models.workflow_runs_cancel_request import (
    WorkflowRunsCancelRequest,
)

from .rest.api.event_api import EventApi
from .rest.api.log_api import LogApi
from .rest.api.step_run_api import StepRunApi
from .rest.api.workflow_api import WorkflowApi
from .rest.api.workflow_run_api import WorkflowRunApi
from .rest.api_client import ApiClient
from .rest.configuration import Configuration
from .rest.models import TriggerWorkflowRunRequest


class RestApi:
    def __init__(self, host: str, api_key: str, tenant_id: str):
        self.tenant_id = tenant_id

        config = Configuration(
            host=host,
            access_token=api_key,
        )

        # Create an instance of the API client
        api_client = ApiClient(configuration=config)
        self.workflow_api = WorkflowApi(api_client)
        self.workflow_run_api = WorkflowRunApi(api_client)
        self.step_run_api = StepRunApi(api_client)
        self.event_api = EventApi(api_client)
        self.log_api = LogApi(api_client)

    def workflow_list(self):
        return self.workflow_api.workflow_list(
            tenant=self.tenant_id,
        )

    def workflow_get(self, workflow_id: str):
        return self.workflow_api.workflow_get(
            workflow=workflow_id,
        )

    def workflow_version_get(self, workflow_id: str, version: str | None = None):
        return self.workflow_api.workflow_version_get(
            workflow=workflow_id,
            version=version,
        )

    def workflow_run_list(
        self,
        workflow_id: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
        event_id: str | None = None,
        additional_metadata: List[StrictStr] | None = None,
    ):
        return self.workflow_api.workflow_run_list(
            tenant=self.tenant_id,
            offset=offset,
            limit=limit,
            workflow_id=workflow_id,
            event_id=event_id,
            additional_metadata=additional_metadata,
        )

    def workflow_run_get(self, workflow_run_id: str):
        return self.workflow_api.workflow_run_get(
            tenant=self.tenant_id,
            workflow_run=workflow_run_id,
        )

    def workflow_run_cancel(self, workflow_run_id: str):
        return self.workflow_run_api.workflow_run_cancel(
            tenant=self.tenant_id,
            workflow_runs_cancel_request=WorkflowRunsCancelRequest(
                workflowRunIds=[workflow_run_id],
            ),
        )

    def workflow_run_create(self, workflow_id: str, input: Dict[str, Any]):
        return self.workflow_run_api.workflow_run_create(
            workflow=workflow_id,
            trigger_workflow_run_request=TriggerWorkflowRunRequest(
                input=input,
            ),
        )

    def list_logs(self, step_run_id: str):
        return self.log_api.log_line_list(
            step_run=step_run_id,
        )

    def events_list(
        self,
        offset: StrictInt | None = None,
        limit: StrictInt | None = None,
        keys: List[StrictStr] | None = None,
        workflows: List[StrictStr] | None = None,
        statuses: List[WorkflowRunStatus] | None = None,
        search: StrictStr | None = None,
        order_by_field: EventOrderByField | None = None,
        order_by_direction: EventOrderByDirection | None = None,
        additional_metadata: List[StrictStr] | None = None,
    ) -> EventList:
        return self.event_api.event_list(
            tenant=self.tenant_id,
            offset=offset,
            limit=limit,
            keys=keys,
            workflows=workflows,
            statuses=statuses,
            search=search,
            order_by_field=order_by_field,
            order_by_direction=order_by_direction,
            additional_metadata=additional_metadata,
        )

    def events_replay(self, event_ids: List[StrictStr] | EventList) -> None:
        if isinstance(event_ids, EventList):
            event_ids = [r.metadata.id for r in event_ids.rows]

        return self.event_api.event_update_replay(
            tenant=self.tenant_id,
            replay_event_request=ReplayEventRequest(eventIds=event_ids),
        )
