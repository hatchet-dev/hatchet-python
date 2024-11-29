import asyncio

from dotenv import load_dotenv

from hatchet_sdk import new_client


async def main():
    load_dotenv()
    hatchet = new_client()

    hatchet.admin.run_workflow(
        "Parent",
        {},
    )


if __name__ == "__main__":
    asyncio.run(main())
