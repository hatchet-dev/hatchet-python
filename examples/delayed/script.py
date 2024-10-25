from datetime import datetime, timedelta
from time import sleep

from dotenv import load_dotenv

from hatchet_sdk import Hatchet

load_dotenv()

hatchet = Hatchet()

scheduled_run = hatchet.admin.schedule_workflow(
    "PrintPrinter",
    [datetime.now() + timedelta(seconds=15)],
    {"message": "test"},
    options={"additional_metadata": {"triggeredBy": "script"}},
)

print("Scheduled run at: " + scheduled_run.trigger_at.ToDatetime().strftime("%Y-%m-%d %H:%M:%S") + "UTC")

sleep(5)

hatchet.rest.scheduled_run_delete(scheduled_run.id)

print("Scheduled run deleted")