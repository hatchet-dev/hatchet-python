from dotenv import load_dotenv
from pydantic import BaseModel

from hatchet_sdk import PushEventOptions, new_client


class ClientPushPayload(BaseModel):
    """Example Pydantic model."""

    test: str


load_dotenv()

client = new_client()
options_model = PushEventOptions(additional_metadata={"hello": "moon"})
payload_model = ClientPushPayload(test="test")

# client.event.push("user:create", {"test": "test"})
client.event.push(
    "user:create",
    payload_model,
    options=options_model,
)
