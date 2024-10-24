from datetime import datetime, timedelta

from dotenv import load_dotenv

from hatchet_sdk import Hatchet

load_dotenv()

hatchet = Hatchet()

hatchet.admin.schedule_workflow(
    "PrintPrinter",
    [datetime.now() + timedelta(seconds=15)],
    {"message": "test"},
    options={"additional_metadata": {"triggeredBy": "script"}},
)
