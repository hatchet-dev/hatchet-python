import asyncio
import time
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor

import grpc
from google.protobuf import timestamp_pb2
from google.protobuf.json_format import MessageToDict, MessageToJson

import hatchet_sdk.v2.hatchet as hatchet
from hatchet_sdk.contracts.dispatcher_pb2 import (
    ActionType,
    AssignedAction,
    HeartbeatRequest,
    WorkerLabels,
    WorkerListenRequest,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
    WorkerUnsubscribeRequest,
)
from hatchet_sdk.contracts.dispatcher_pb2_grpc import DispatcherStub

import hatchet_sdk.connection as connection


@dataclass
class WorkerOptions:
    name: str
    actions: List[str]
    slots: int = 5
    debug: bool = False
    labels: Dict[str, str | int] = field(default_factory=dict)
    heartbeat: int = 4  # heartbeat period in seconds

    @property
    def labels_proto(self) -> Dict[str, WorkerLabels]:
        ret = dict()
        for k, v in self.labels.items():
            if isinstance(v, int):
                ret[k] = WorkerLabels(intValue=v)
            else:
                ret[k] = WorkerLabels(strValue=str(v))
        return ret


class WorkerStatus(Enum):
    UNKNOWN = 1
    REGISTERED = 2
    # STARTING = 2
    HEALTHY = 3
    UNHEALTHY = 4


class _HeartBeater:
    def __init__(self, worker: "Worker"):
        self.worker = worker
        self.last_heartbeat: int = -1  # unix epoch in seconds
        self.missed = 0
        self.error = 0

    async def heartbeat(self):
        while not self.worker._shutdown:
            now = int(time.time())
            proto = HeartbeatRequest(
                workerId=self.worker.id,
                heartbeatAt=timestamp_pb2.Timestamp(seconds=now),
            )
            try:
                resp = self.worker.client.dispatcher.client.Heartbeat(
                    proto, timeout=5, metadata=self.worker._grpc_metadata()
                )
                self.worker.client.logger.info(f"heartbeat: {MessageToJson(resp)}")
            except grpc.RpcError as e:
                self.error += 1

            if self.last_heartbeat < 0:
                self.last_heartbeat = now
                self.status = WorkerStatus.HEALTHY
            else:
                diff = proto.heartbeatAt.seconds - self.last_heartbeat
                if diff > self.worker.options.heartbeat:
                    self.missed += 1

            await asyncio.sleep(self.worker.options.heartbeat)


class _Listner:
    def __init__(self, worker: "Worker"):
        self.worker = worker
        self.attempt = 0

        conn = connection.new_conn(self.worker.client.config, aio=True)
        self.stub = DispatcherStub(conn)

    async def listen(self) -> AsyncGenerator[AssignedAction]:
        resp = None
        try:
            while not self.worker._shutdown:
                proto = WorkerListenRequest(workerId=self.worker.id)
                print(repr(asyncio.get_running_loop()))
                resp = self.stub.ListenV2(
                    proto, timeout=5, metadata=self.worker._grpc_metadata()
                )
                self.worker.client.logger.info("listening")
                async for event in resp:
                    yield event
                    if self.worker._shutdown:
                        resp.cancel()
                        resp = None
                        break
                resp = None
                self.attempt += 1
        except Exception as e:
            self.worker.client.logger.info(e)
            raise e
        finally:
            if resp:
                resp.cancel()


class Worker:

    def __init__(
        self,
        client: "hatchet.Hatchet",
        options: WorkerOptions,
    ):
        self.options = options
        self.client = client
        self.status = WorkerStatus.UNKNOWN
        self.id: Optional[str] = None

        self._shutdown = False  # flag for shutting down
        self._heartbeater = _HeartBeater(self)
        self._heartbeater_task: Optional[asyncio.Task] = None
        self._listener = _Listner(self)
        self._listener_task: Optional[asyncio.Task] = None

    def _register(self) -> str:
        resp: WorkerRegisterResponse = self.client.dispatcher.client.Register(
            self._to_register_proto(),
            timeout=30,
            metadata=self._grpc_metadata(),
        )
        self.client.logger.info(f"registered: {MessageToDict(resp)}")
        self.id = resp.workerId
        self.status = WorkerStatus.REGISTERED
        return resp.workerId

    async def start(self):
        self._register()
        # self._heartbeat_task = asyncio.create_task(
        #     self._heartbeater.heartbeat(), name="heartbeat"
        # )
        agen = self._listener.listen()
        self._listener_task = asyncio.create_task(self._onevent(agen), name="listner")
        # while True:
        #     if self._heartbeater.last_heartbeat > 0:
        #         return
        #     await asyncio.sleep(0.1)

    async def shutdown(self):
        print("shutting down")
        self._shutdown = True
        # self._listener_task.cancel()
        # self._heartbeat_task.cancel()
        await asyncio.gather(self._heartbeat_task, self._listener_task)

    async def _onevent(self, agen: AsyncGenerator[AssignedAction]):
        self.client.logger.info(repr(agen))
        try:
            async for action in agen:
                print(MessageToDict(action))
        except Exception as e:
            print(e)
            raise
        finally:
            pass

    def _grpc_metadata(self):
        return [("authorization", f"bearer {self.client.config.token}")]

    def _to_register_proto(self) -> WorkerRegisterRequest:
        options = self.options
        proto = WorkerRegisterRequest(
            workerName=options.name,
            services=["default"],
            actions=list(options.actions),
            maxRuns=options.slots,
            labels=options.labels_proto,
        )
        return proto
