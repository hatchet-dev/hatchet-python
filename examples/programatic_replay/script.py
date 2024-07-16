from dotenv import load_dotenv

from hatchet_sdk import Hatchet
from hatchet_sdk.clients.rest.models.workflow_run_status import WorkflowRunStatus

load_dotenv()

hatchet = Hatchet(debug=True)

if __name__ == "__main__":
    # Look up the failed workflow runs
    failed = hatchet.client.rest.events_list(
        statuses=[WorkflowRunStatus.FAILED],
        limit=3
        )

    # Replay the failed workflow runs
    retried = hatchet.client.rest.events_replay(
        failed
    )
