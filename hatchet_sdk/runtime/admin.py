
# import hatchet_sdk.v2.callable as sdk
# import hatchet_sdk.clients.admin as client

# from hatchet_sdk.contracts.workflows_pb2 import (
#     CreateStepRateLimit,
#     CreateWorkflowJobOpts,
#     CreateWorkflowStepOpts,
#     CreateWorkflowVersionOpts,
#     DesiredWorkerLabels,
#     StickyStrategy,
#     WorkflowConcurrencyOpts,
#     WorkflowKind,
# )

# async def put_workflow(callable: sdk.HatchetCallable, client: client.AdminClient):
#     options = callable._.options

#     kind: WorkflowKind = WorkflowKind.DURABLE if options.durable else WorkflowKind.FUNCTION

