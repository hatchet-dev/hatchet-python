import os
import sys
from typing import Any, Callable, Dict, List

from hatchet_sdk.client import Client
from hatchet_sdk.clients.cloud_rest.models.create_managed_worker_runtime_config_request import CreateManagedWorkerRuntimeConfigRequest
from hatchet_sdk.clients.cloud_rest.models.infra_as_code_request import InfraAsCodeRequest
from hatchet_sdk.clients.cloud_rest.models.managed_worker_region import ManagedWorkerRegion
from hatchet_sdk.compute.configs import Compute
from hatchet_sdk.logger import logger

class ManagedCompute:
    def __init__(self, actions: Dict[str, Callable[..., Any]], client: Client):
        self.actions = actions
        self.client = client
        self.action_map = self.get_action_map(self.actions)

        self.cloud_register_enabled = os.environ.get("HATCHET_CLOUD_REGISTER_ID")

        if self.cloud_register_enabled is None:
            logger.warning("🚫 Local mode detected, skipping cloud registration and running all actions locally.")

            logger.warning("Managed Cloud Compute Plan:")
            for _, compute in self.action_map.items():
                logger.warning(f"    ----------------------------")
                logger.warning(f"    Actions: {', '.join(compute.actions)}")
                logger.warning(f"    Num Replicas: {compute.num_replicas}")
                logger.warning(f"    CPU Kind: {compute.cpu_kind}")
                logger.warning(f"    CPUs: {compute.cpus}")
                logger.warning(f"    Memory MB: {compute.memory_mb}")
                logger.warning(f"    Region: {compute.region}")

            logger.warning("🚫 Local mode detected, skipping cloud registration and running all actions locally.")


    def get_action_map(self, actions: Dict[str, Callable[..., Any]]) -> Dict[str, CreateManagedWorkerRuntimeConfigRequest]:
        '''
        Builds a map of compute hashes to compute configs and lists of actions that correspond to each compute hash.
        '''
        map: Dict[str, CreateManagedWorkerRuntimeConfigRequest] = {}
    
        for action, func in actions.items():
            try:
                compute = func._step_compute
                key = compute.hash()
                if key not in map:
                    map[key] = CreateManagedWorkerRuntimeConfigRequest(
                        actions=[],
                        num_replicas=1,
                        cpu_kind = compute.cpu_kind,
                        cpus = compute.cpus,
                        memory_mb = compute.memory_mb,
                        region = compute.region
                    )
                map[key].actions.append(action)
            except Exception as e:
                logger.error(f"Error getting compute for action {action}: {e}")
        return map

    async def cloud_register(self):
        # if the environment variable HATCHET_CLOUD_REGISTER_ID is set, use it and exit
        if self.cloud_register_enabled is not None:
            logger.info(f"Registering cloud compute plan with ID: {self.cloud_register_enabled}")
            
            try:
                configs = list(self.action_map.values())

                if len(configs) == 0:
                    logger.warning("No actions to register, skipping cloud registration.")
                    return

                req = InfraAsCodeRequest(
                    runtime_configs = configs
                )

                res = await self.client.rest.aio.managed_worker_api.infra_as_code_create(
                    infra_as_code_request=self.cloud_register_enabled,
                    infra_as_code_request2=req,
                    _request_timeout=10,
                )

                sys.exit(0)
            except Exception as e:
                print("ERROR", e)
                sys.exit(1)