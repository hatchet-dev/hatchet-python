import functools
from typing import Any, Callable, List, Tuple

from hatchet_sdk.contracts.workflows_pb2 import (
    CreateWorkflowJobOpts,
    CreateWorkflowStepOpts,
    CreateWorkflowVersionOpts,
    WorkflowConcurrencyOpts,
    WorkflowKind,
)

stepsType = List[Tuple[str, Callable[..., Any]]]


class WorkflowMeta(type):
    def __new__(cls, name, bases, attrs):
        concurrencyActions: stepsType = [
            (getattr(func, "_concurrency_fn_name"), attrs.pop(func_name))
            for func_name, func in list(attrs.items())
            if hasattr(func, "_concurrency_fn_name")
        ]
        steps: stepsType = [
            (getattr(func, "_step_name"), attrs.pop(func_name))
            for func_name, func in list(attrs.items())
            if hasattr(func, "_step_name")
        ]
        onFailureSteps: stepsType = [
            (getattr(func, "_on_failure_step_name"), attrs.pop(func_name))
            for func_name, func in list(attrs.items())
            if hasattr(func, "_on_failure_step_name")
        ]

        # Define __init__ and get_step_order methods
        original_init = attrs.get("__init__")  # Get the original __init__ if it exists

        def __init__(self, *args, **kwargs):
            if original_init:
                original_init(self, *args, **kwargs)  # Call original __init__

        def get_service_name(namespace: str) -> str:
            return f"{namespace}{name.lower()}"

        @functools.cache
        def get_actions(self, namespace: str) -> stepsType:
            serviceName = get_service_name(namespace)
            func_actions = [
                (serviceName + ":" + func_name, func) for func_name, func in steps
            ]
            concurrency_actions = [
                (serviceName + ":" + func_name, func)
                for func_name, func in concurrencyActions
            ]
            onFailure_actions = [
                (serviceName + ":" + func_name, func)
                for func_name, func in onFailureSteps
            ]

            return func_actions + concurrency_actions + onFailure_actions

        # Add these methods and steps to class attributes
        attrs["__init__"] = __init__
        attrs["get_actions"] = get_actions

        for step_name, step_func in steps:
            attrs[step_name] = step_func

        def get_name(self, namespace: str):
            return namespace + attrs["name"]

        attrs["get_name"] = get_name

        cron_triggers = attrs["on_crons"]
        version = attrs["version"]
        schedule_timeout = attrs["schedule_timeout"]
        sticky = attrs["sticky"]

        @functools.cache
        def get_create_opts(self, namespace: str):
            serviceName = get_service_name(namespace)
            name = self.get_name(namespace)
            event_triggers = [namespace + event for event in attrs["on_events"]]
            createStepOpts: List[CreateWorkflowStepOpts] = [
                CreateWorkflowStepOpts(
                    readable_id=step_name,
                    action=serviceName + ":" + step_name,
                    timeout=func._step_timeout or "60s",
                    inputs="{}",
                    parents=[x for x in func._step_parents],
                    retries=func._step_retries,
                    rate_limits=func._step_rate_limits,
                    worker_labels=func._step_desired_worker_labels,
                )
                for step_name, func in steps
            ]

            concurrency: WorkflowConcurrencyOpts | None = None

            if len(concurrencyActions) > 0:
                action = concurrencyActions[0]

                concurrency = WorkflowConcurrencyOpts(
                    action=serviceName + ":" + action[0],
                    max_runs=action[1]._concurrency_max_runs,
                    limit_strategy=action[1]._concurrency_limit_strategy,
                )

            on_failure_job: List[CreateWorkflowJobOpts] | None = None

            if len(onFailureSteps) > 0:
                func_name, func = onFailureSteps[0]
                on_failure_job = CreateWorkflowJobOpts(
                    name=name + "-on-failure",
                    steps=[
                        CreateWorkflowStepOpts(
                            readable_id=func_name,
                            action=serviceName + ":" + func_name,
                            timeout=func._on_failure_step_timeout or "60s",
                            inputs="{}",
                            parents=[],
                            retries=func._on_failure_step_retries,
                            rate_limits=func._on_failure_step_rate_limits,
                        )
                    ],
                )

            return CreateWorkflowVersionOpts(
                name=name,
                kind=WorkflowKind.DAG,
                version=version,
                event_triggers=event_triggers,
                cron_triggers=cron_triggers,
                schedule_timeout=schedule_timeout,
                sticky=sticky,
                jobs=[
                    CreateWorkflowJobOpts(
                        name=name,
                        steps=createStepOpts,
                    )
                ],
                on_failure_job=on_failure_job,
                concurrency=concurrency,
            )

        attrs["get_create_opts"] = get_create_opts

        return super(WorkflowMeta, cls).__new__(cls, name, bases, attrs)
