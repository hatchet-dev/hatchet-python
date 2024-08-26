from typing import TypedDict

from dotenv import load_dotenv

from hatchet_sdk import Context
from hatchet_sdk.v2.hatchet import Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


class MyResultType(TypedDict):
    return_string: str


@hatchet.function(default_priority=1)
def high_prio_func(context: Context) -> MyResultType:
    return MyResultType(return_string="High Priority Return")


@hatchet.function(default_priority=2)
def low_prio_func(context: Context) -> MyResultType:
    return MyResultType(return_string="Low Priority Return")


def main():
    worker = hatchet.worker("example-priority-worker", max_runs=1)

    worker.start()


if __name__ == "__main__":
    main()
