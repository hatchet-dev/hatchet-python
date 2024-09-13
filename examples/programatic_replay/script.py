import asyncio

from dotenv import load_dotenv

from hatchet_sdk import Hatchet, WorkflowRunStatus

load_dotenv()

hatchet = Hatchet(debug=True)


async def main():
    hatchet.admin.run_workflow("AsyncWorkflow", {}, {
        "namespace": "async",
    })
    # Look up the failed workflow runs
    # failed = await hatchet.rest.aio.workflow_run_list(
    #     statuses=[WorkflowRunStatus.FAILED], limit=3
    # )
    # # Replay the failed workflow runs
    # retried = await hatchet.rest.aio.workflow_run_replay(
    #     workflow_run_ids=[run.metadata.id for run in failed.rows]
    # )


if __name__ == "__main__":
    asyncio.run(main())
