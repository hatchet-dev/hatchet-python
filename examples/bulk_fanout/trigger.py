import asyncio

from dotenv import load_dotenv

from hatchet_sdk import new_client


async def main():
    load_dotenv()
    hatchet = new_client()

    hatchet.event.push(
        "parent:create", {"n": 999}, {"additional_metadata": {"no-dedupe": "world"}}
    )


if __name__ == "__main__":
    asyncio.run(main())
