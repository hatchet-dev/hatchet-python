from examples.simple.worker import MyWorkflow
from hatchet_sdk.hatchet import Hatchet
import asyncio
import pytest
import logging

@pytest.mark.asyncio
async def test_simple_e2e():
    logging.info("Starting test")

    hatchet = Hatchet(debug=True)
    workflow = MyWorkflow()
    worker = hatchet.worker("test-worker", max_runs=4)
    worker.register_workflow(workflow)

    async def run_worker():
        try:
            await worker.start()
        except SystemExit:
            pass

    logging.info("Starting worker")
    worker_task = asyncio.create_task(run_worker())
    logging.info("worker task started, running workflow")

    workflow_id = hatchet.client.admin.run_workflow("MyWorkflow", {})
    logging.info(f"Workflow ID: {workflow_id}")

    await asyncio.sleep(1)

    for i in range(10):
        res = hatchet.client.rest.workflow_get(workflow_id)
        logging.info(f"Workflow status: {res['status']}")
        if res["status"] == "COMPLETED":
            break
        await asyncio.sleep(10)


    logging.info("Waiting for worker to complete")

    await asyncio.sleep(1)
    
    await asyncio.wait_for(worker_task, timeout=10)
    logging.info("Worker completed")

    assert res['status'] == "COMPLETED"