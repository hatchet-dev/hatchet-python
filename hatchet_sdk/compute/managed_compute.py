import os
import sys
from typing import Any, Callable, Dict, List

from hatchet_sdk.client import Client
from hatchet_sdk.clients.cloud_rest.models.infra_as_code_create_request import (
    InfraAsCodeCreateRequest,
)
from hatchet_sdk.clients.cloud_rest.models.managed_worker_create_request_runtime_config import (
    ManagedWorkerCreateRequestRuntimeConfig,
)
from hatchet_sdk.compute.configs import Compute
from hatchet_sdk.logger import logger

# TODO why is this not generating
# from hatchet_sdk.clients.cloud_rest.models.managed_worker_create_request import ManagedWorkerRegion


class ManagedCompute:
    def __init__(
        self, actions: Dict[str, Callable[..., Any]], client: Client, max_runs: int = 1
    ):
        self.actions = actions
        self.client = client
        self.max_runs = max_runs
        self.configs = self.get_compute_configs(self.actions)
        self.cloud_register_enabled = os.environ.get("HATCHET_CLOUD_REGISTER_ID")

        if len(self.configs) == 0:
            logger.debug(
                "no compute configs found, skipping cloud registration and running all actions locally."
            )
            return

        if self.cloud_register_enabled is None:
            logger.warning("managed cloud compute plan:")
            for compute in self.configs:
                logger.warning(f"    ----------------------------")
                logger.warning(f"    actions: {', '.join(compute.actions)}")
                logger.warning(f"    num replicas: {compute.num_replicas}")
                logger.warning(f"    cpu kind: {compute.cpu_kind}")
                logger.warning(f"    cpus: {compute.cpus}")
                logger.warning(f"    memory mb: {compute.memory_mb}")
                logger.warning(f"    regions: {compute.regions}")

            logger.warning(
                "NOTICE: local mode detected, skipping cloud registration and running all actions locally."
            )

    def get_compute_configs(
        self, actions: Dict[str, Callable[..., Any]]
    ) -> List[ManagedWorkerCreateRequestRuntimeConfig]:
        """
        Builds a map of compute hashes to compute configs and lists of actions that correspond to each compute hash.
        """
        map: Dict[str, ManagedWorkerCreateRequestRuntimeConfig] = {}

        try:
            for action, func in actions.items():
                compute = func._step_compute

                if compute is None:
                    continue

                key = compute.hash()
                if key not in map:
                    map[key] = ManagedWorkerCreateRequestRuntimeConfig(
                        actions=[],
                        num_replicas=1,
                        cpu_kind=compute.cpu_kind,
                        cpus=compute.cpus,
                        memory_mb=compute.memory_mb,
                        regions=compute.regions,
                        slots=self.max_runs,
                    )
                map[key].actions.append(action)

            return list(map.values())
        except Exception as e:
            logger.error(f"Error getting compute configs: {e}")
            return []

    async def cloud_register(self):

        # if the environment variable HATCHET_CLOUD_REGISTER_ID is set, use it and exit
        if self.cloud_register_enabled is not None:
            logger.info(
                f"Registering cloud compute plan with ID: {self.cloud_register_enabled}"
            )

            try:
                if len(self.configs) == 0:
                    logger.warning(
                        "No actions to register, skipping cloud registration."
                    )
                    return

                req = InfraAsCodeCreateRequest(runtime_configs=self.configs)

                res = (
                    await self.client.rest.aio.managed_worker_api.infra_as_code_create(
                        infra_as_code_request=self.cloud_register_enabled,
                        infra_as_code_create_request=req,
                        _request_timeout=10,
                    )
                )

                sys.exit(0)
            except Exception as e:
                print("ERROR", e)
                sys.exit(1)
