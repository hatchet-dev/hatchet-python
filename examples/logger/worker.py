from logging import getLogger

from dotenv import load_dotenv

from examples.logger.client import hatchet
from examples.logger.workflow import LoggingWorkflow

worker = hatchet.worker("test-worker", max_runs=5)

workflow = LoggingWorkflow()
worker.register_workflow(workflow)

worker.start()
