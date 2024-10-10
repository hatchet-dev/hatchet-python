import asyncio
import base64
import json
import os

from dotenv import load_dotenv

from hatchet_sdk import new_client
from hatchet_sdk.clients.admin import TriggerWorkflowOptions
from hatchet_sdk.clients.rest.models.workflow_run import WorkflowRun
from hatchet_sdk.clients.run_event_listener import StepRunEventType


async def main():
    load_dotenv()
    hatchet = new_client()

    workflowRuns: WorkflowRun = []

    for i in range(100):
        workflowRuns.append(
            {
                "workflow_name": "Child",
                "input": {"a": str(i)},
                "options": {"additional_metadata": {"hello": "earth",  "dedupe": "dedupe2"},},
            }
        )

    workflowRunRefs = hatchet.admin.run_workflows(
        workflowRuns,
    )

    results = await asyncio.gather(
        *[workflowRunRef.result() for workflowRunRef in workflowRunRefs],
        return_exceptions=True,
    )

    for result in results:
        if isinstance(result, Exception):
            print(f"An error occurred: {result}")  # Handle the exception here
        else:
            print(result)


if __name__ == "__main__":
    asyncio.run(main())
