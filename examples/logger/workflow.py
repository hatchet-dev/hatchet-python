import logging
import time

from examples.logger.client import hatchet
from hatchet_sdk.context import Context

logger = logging.getLogger(__name__)


@hatchet.workflow(on_crons=["* * * * *"])
class LoggingWorkflow:
    @hatchet.step()
    def step1(self, context: Context):
        for i in range(12):
            logger.info("executed step1 - {}".format(i))
            time.sleep(1)
        return {"status": "success"}
