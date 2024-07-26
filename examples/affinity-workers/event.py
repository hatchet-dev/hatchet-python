from dotenv import load_dotenv

from hatchet_sdk import PushEventOptions, new_client

load_dotenv()

client = new_client()

client.event.push(
    "affinity:run",
    {"test": "test"},
    options={"additional_metadata": {"hello": "moon"}},
)
