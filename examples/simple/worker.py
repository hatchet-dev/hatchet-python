import json
import time
from logging import Logger

from dotenv import load_dotenv

from hatchet_sdk import Context, CreateWorkflowVersionOpts, Hatchet

import logging

from hatchet_sdk.loader import ClientConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()

hatchet = Hatchet(
    debug=True,
    config=ClientConfig(
        logger=logger
    )
)


@hatchet.workflow(on_crons=["* * * * *"])
class ParentCron:
    @hatchet.step()
    def step1(self, context: Context):
        for i in range(12):
            logger.info("executed step1 - {}".format(i))
            time.sleep(5)
        return {
            "status": "success"
        }


worker = hatchet.worker("test-worker", max_runs=5)

workflow = ParentCron()
worker.register_workflow(workflow)


worker.start()
