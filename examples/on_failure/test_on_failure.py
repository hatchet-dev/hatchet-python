import asyncio

import pytest

from hatchet_sdk import Hatchet
from hatchet_sdk.clients.rest.models.job_run_status import JobRunStatus
from tests.utils import fixture_bg_worker

worker = fixture_bg_worker(["poetry", "run", "on_failure"])


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run_timeout(hatchet: Hatchet):
    run = hatchet.admin.run_workflow("OnFailureWorkflow", {})
    try:
        await run.result()

        assert False, "Expected workflow to timeout"
    except Exception as e:
        assert "step1 failed" in str(e)

    await asyncio.sleep(5)  # Wait for the on_failure job to finish

    job_runs = hatchet.rest.workflow_run_get(run.workflow_run_id).job_runs
    assert len(job_runs) == 2

    successful_job_runs = [jr for jr in job_runs if jr.status == JobRunStatus.SUCCEEDED]
    failed_job_runs = [jr for jr in job_runs if jr.status == JobRunStatus.FAILED]

    assert len(successful_job_runs) == 1
    assert len(failed_job_runs) == 1
