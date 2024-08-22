import asyncio

from dotenv import load_dotenv

from hatchet_sdk import Hatchet, WorkflowList

load_dotenv()

hatchet = Hatchet(debug=True)


async def main():
    hatchet_async_rest_client = await hatchet.get_async_rest_client()
    list: WorkflowList = await hatchet_async_rest_client.workflow_list()

    for workflow in list.rows:
        print(workflow.name)
        print(workflow.metadata.id)
        print(workflow.metadata.created_at)
        print(workflow.metadata.updated_at)


if __name__ == "__main__":
    asyncio.run(main())
