# relative imports
import os
from typing import Any

import grpc

from hatchet_sdk.clients.run_event_listener import RunEventListenerClient
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener
from hatchet_sdk.connection import new_conn

from .clients.admin import AdminClientImpl, new_admin
from .clients.dispatcher import DispatcherClientImpl, new_dispatcher
from .clients.events import EventClientImpl, new_event
from .clients.rest.api.workflow_api import WorkflowApi
from .clients.rest.api.workflow_run_api import WorkflowRunApi
from .clients.rest.api_client import ApiClient
from .clients.rest.configuration import Configuration
from .clients.rest_client import RestApi
from .loader import ClientConfig, ConfigLoader


class Client:
    admin: AdminClientImpl
    dispatcher: DispatcherClientImpl
    event: EventClientImpl
    rest: RestApi
    workflow_listener: PooledWorkflowRunListener


class ClientImpl(Client):
    def __init__(
        self,
        event_client: EventClientImpl,
        admin_client: AdminClientImpl,
        dispatcher_client: DispatcherClientImpl,
        workflow_listener: PooledWorkflowRunListener,
        rest_client: RestApi,
        config: ClientConfig,
    ):
        self.admin = admin_client
        self.dispatcher = dispatcher_client
        self.event = event_client
        self.rest = rest_client
        self.config = config
        self.listener = RunEventListenerClient(config)
        self.workflow_listener = workflow_listener

def with_host_port(host: str, port: int):
    def with_host_port_impl(config: ClientConfig):
        config.host = host
        config.port = port

    return with_host_port_impl


def new_client(defaults: ClientConfig = {}, *opts_functions) -> ClientImpl:
    config: ClientConfig = ConfigLoader(".").load_client_config(defaults)

    for opt_function in opts_functions:
        opt_function(config)

    if config.tls_config is None:
        raise ValueError("TLS config is required")

    if config.host_port is None:
        raise ValueError("Host and port are required")

    conn: grpc.Channel = new_conn(config)

    # Instantiate client implementations
    event_client = new_event(conn, config)
    admin_client = new_admin(config)
    dispatcher_client = new_dispatcher(config)
    rest_client = RestApi(config.server_url, config.token, config.tenant_id)
    workflow_listener_client = None

    return ClientImpl(
        event_client,
        admin_client,
        dispatcher_client,
        workflow_listener_client,
        rest_client,
        config,
    )
