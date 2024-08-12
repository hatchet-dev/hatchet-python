from dotenv import load_dotenv
from hatchet_sdk.v2.hatchet import Hatchet
from hatchet_sdk.v2.callable import DurableContext
from hatchet_sdk import Context

load_dotenv()

hc = Hatchet(debug=True)