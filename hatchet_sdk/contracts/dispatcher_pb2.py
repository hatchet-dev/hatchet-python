# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: dispatcher.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10\x64ispatcher.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"V\n\x0cWorkerLabels\x12\x15\n\x08strValue\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x15\n\x08intValue\x18\x02 \x01(\x05H\x01\x88\x01\x01\x42\x0b\n\t_strValueB\x0b\n\t_intValue\"\x88\x02\n\x15WorkerRegisterRequest\x12\x12\n\nworkerName\x18\x01 \x01(\t\x12\x0f\n\x07\x61\x63tions\x18\x02 \x03(\t\x12\x10\n\x08services\x18\x03 \x03(\t\x12\x14\n\x07maxRuns\x18\x04 \x01(\x05H\x00\x88\x01\x01\x12\x32\n\x06labels\x18\x05 \x03(\x0b\x32\".WorkerRegisterRequest.LabelsEntry\x12\x16\n\twebhookId\x18\x06 \x01(\tH\x01\x88\x01\x01\x1a<\n\x0bLabelsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x1c\n\x05value\x18\x02 \x01(\x0b\x32\r.WorkerLabels:\x02\x38\x01\x42\n\n\x08_maxRunsB\x0c\n\n_webhookId\"P\n\x16WorkerRegisterResponse\x12\x10\n\x08tenantId\x18\x01 \x01(\t\x12\x10\n\x08workerId\x18\x02 \x01(\t\x12\x12\n\nworkerName\x18\x03 \x01(\t\"\xa3\x01\n\x19UpsertWorkerLabelsRequest\x12\x10\n\x08workerId\x18\x01 \x01(\t\x12\x36\n\x06labels\x18\x02 \x03(\x0b\x32&.UpsertWorkerLabelsRequest.LabelsEntry\x1a<\n\x0bLabelsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x1c\n\x05value\x18\x02 \x01(\x0b\x32\r.WorkerLabels:\x02\x38\x01\"@\n\x1aUpsertWorkerLabelsResponse\x12\x10\n\x08tenantId\x18\x01 \x01(\t\x12\x10\n\x08workerId\x18\x02 \x01(\t\"\x86\x04\n\x0e\x41ssignedAction\x12\x10\n\x08tenantId\x18\x01 \x01(\t\x12\x15\n\rworkflowRunId\x18\x02 \x01(\t\x12\x18\n\x10getGroupKeyRunId\x18\x03 \x01(\t\x12\r\n\x05jobId\x18\x04 \x01(\t\x12\x0f\n\x07jobName\x18\x05 \x01(\t\x12\x10\n\x08jobRunId\x18\x06 \x01(\t\x12\x0e\n\x06stepId\x18\x07 \x01(\t\x12\x11\n\tstepRunId\x18\x08 \x01(\t\x12\x10\n\x08\x61\x63tionId\x18\t \x01(\t\x12\x1f\n\nactionType\x18\n \x01(\x0e\x32\x0b.ActionType\x12\x15\n\ractionPayload\x18\x0b \x01(\t\x12\x10\n\x08stepName\x18\x0c \x01(\t\x12\x12\n\nretryCount\x18\r \x01(\x05\x12 \n\x13\x61\x64\x64itional_metadata\x18\x0e \x01(\tH\x00\x88\x01\x01\x12!\n\x14\x63hild_workflow_index\x18\x0f \x01(\x05H\x01\x88\x01\x01\x12\x1f\n\x12\x63hild_workflow_key\x18\x10 \x01(\tH\x02\x88\x01\x01\x12#\n\x16parent_workflow_run_id\x18\x11 \x01(\tH\x03\x88\x01\x01\x42\x16\n\x14_additional_metadataB\x17\n\x15_child_workflow_indexB\x15\n\x13_child_workflow_keyB\x19\n\x17_parent_workflow_run_id\"\'\n\x13WorkerListenRequest\x12\x10\n\x08workerId\x18\x01 \x01(\t\",\n\x18WorkerUnsubscribeRequest\x12\x10\n\x08workerId\x18\x01 \x01(\t\"?\n\x19WorkerUnsubscribeResponse\x12\x10\n\x08tenantId\x18\x01 \x01(\t\x12\x10\n\x08workerId\x18\x02 \x01(\t\"\xe1\x01\n\x13GroupKeyActionEvent\x12\x10\n\x08workerId\x18\x01 \x01(\t\x12\x15\n\rworkflowRunId\x18\x02 \x01(\t\x12\x18\n\x10getGroupKeyRunId\x18\x03 \x01(\t\x12\x10\n\x08\x61\x63tionId\x18\x04 \x01(\t\x12\x32\n\x0e\x65ventTimestamp\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12+\n\teventType\x18\x06 \x01(\x0e\x32\x18.GroupKeyActionEventType\x12\x14\n\x0c\x65ventPayload\x18\x07 \x01(\t\"\xec\x01\n\x0fStepActionEvent\x12\x10\n\x08workerId\x18\x01 \x01(\t\x12\r\n\x05jobId\x18\x02 \x01(\t\x12\x10\n\x08jobRunId\x18\x03 \x01(\t\x12\x0e\n\x06stepId\x18\x04 \x01(\t\x12\x11\n\tstepRunId\x18\x05 \x01(\t\x12\x10\n\x08\x61\x63tionId\x18\x06 \x01(\t\x12\x32\n\x0e\x65ventTimestamp\x18\x07 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\'\n\teventType\x18\x08 \x01(\x0e\x32\x14.StepActionEventType\x12\x14\n\x0c\x65ventPayload\x18\t \x01(\t\"9\n\x13\x41\x63tionEventResponse\x12\x10\n\x08tenantId\x18\x01 \x01(\t\x12\x10\n\x08workerId\x18\x02 \x01(\t\"\xc0\x01\n SubscribeToWorkflowEventsRequest\x12\x1a\n\rworkflowRunId\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x1e\n\x11\x61\x64\x64itionalMetaKey\x18\x02 \x01(\tH\x01\x88\x01\x01\x12 \n\x13\x61\x64\x64itionalMetaValue\x18\x03 \x01(\tH\x02\x88\x01\x01\x42\x10\n\x0e_workflowRunIdB\x14\n\x12_additionalMetaKeyB\x16\n\x14_additionalMetaValue\"7\n\x1eSubscribeToWorkflowRunsRequest\x12\x15\n\rworkflowRunId\x18\x01 \x01(\t\"\xb2\x02\n\rWorkflowEvent\x12\x15\n\rworkflowRunId\x18\x01 \x01(\t\x12#\n\x0cresourceType\x18\x02 \x01(\x0e\x32\r.ResourceType\x12%\n\teventType\x18\x03 \x01(\x0e\x32\x12.ResourceEventType\x12\x12\n\nresourceId\x18\x04 \x01(\t\x12\x32\n\x0e\x65ventTimestamp\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x14\n\x0c\x65ventPayload\x18\x06 \x01(\t\x12\x0e\n\x06hangup\x18\x07 \x01(\x08\x12\x18\n\x0bstepRetries\x18\x08 \x01(\x05H\x00\x88\x01\x01\x12\x17\n\nretryCount\x18\t \x01(\x05H\x01\x88\x01\x01\x42\x0e\n\x0c_stepRetriesB\r\n\x0b_retryCount\"\xa8\x01\n\x10WorkflowRunEvent\x12\x15\n\rworkflowRunId\x18\x01 \x01(\t\x12(\n\teventType\x18\x02 \x01(\x0e\x32\x15.WorkflowRunEventType\x12\x32\n\x0e\x65ventTimestamp\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x1f\n\x07results\x18\x04 \x03(\x0b\x32\x0e.StepRunResult\"\x8a\x01\n\rStepRunResult\x12\x11\n\tstepRunId\x18\x01 \x01(\t\x12\x16\n\x0estepReadableId\x18\x02 \x01(\t\x12\x10\n\x08jobRunId\x18\x03 \x01(\t\x12\x12\n\x05\x65rror\x18\x04 \x01(\tH\x00\x88\x01\x01\x12\x13\n\x06output\x18\x05 \x01(\tH\x01\x88\x01\x01\x42\x08\n\x06_errorB\t\n\x07_output\"W\n\rOverridesData\x12\x11\n\tstepRunId\x18\x01 \x01(\t\x12\x0c\n\x04path\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\t\x12\x16\n\x0e\x63\x61llerFilename\x18\x04 \x01(\t\"\x17\n\x15OverridesDataResponse\"U\n\x10HeartbeatRequest\x12\x10\n\x08workerId\x18\x01 \x01(\t\x12/\n\x0bheartbeatAt\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\"\x13\n\x11HeartbeatResponse\"F\n\x15RefreshTimeoutRequest\x12\x11\n\tstepRunId\x18\x01 \x01(\t\x12\x1a\n\x12incrementTimeoutBy\x18\x02 \x01(\t\"G\n\x16RefreshTimeoutResponse\x12-\n\ttimeoutAt\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\"\'\n\x12ReleaseSlotRequest\x12\x11\n\tstepRunId\x18\x01 \x01(\t\"\x15\n\x13ReleaseSlotResponse*N\n\nActionType\x12\x12\n\x0eSTART_STEP_RUN\x10\x00\x12\x13\n\x0f\x43\x41NCEL_STEP_RUN\x10\x01\x12\x17\n\x13START_GET_GROUP_KEY\x10\x02*\xa2\x01\n\x17GroupKeyActionEventType\x12 \n\x1cGROUP_KEY_EVENT_TYPE_UNKNOWN\x10\x00\x12 \n\x1cGROUP_KEY_EVENT_TYPE_STARTED\x10\x01\x12\"\n\x1eGROUP_KEY_EVENT_TYPE_COMPLETED\x10\x02\x12\x1f\n\x1bGROUP_KEY_EVENT_TYPE_FAILED\x10\x03*\xca\x01\n\x13StepActionEventType\x12\x1b\n\x17STEP_EVENT_TYPE_UNKNOWN\x10\x00\x12\x1b\n\x17STEP_EVENT_TYPE_STARTED\x10\x01\x12\x1d\n\x19STEP_EVENT_TYPE_COMPLETED\x10\x02\x12\x1a\n\x16STEP_EVENT_TYPE_FAILED\x10\x03\x12 \n\x1cSTEP_EVENT_TYPE_ACKNOWLEDGED\x10\x04\x12\x1c\n\x18STEP_EVENT_TYPE_REJECTED\x10\x05*e\n\x0cResourceType\x12\x19\n\x15RESOURCE_TYPE_UNKNOWN\x10\x00\x12\x1a\n\x16RESOURCE_TYPE_STEP_RUN\x10\x01\x12\x1e\n\x1aRESOURCE_TYPE_WORKFLOW_RUN\x10\x02*\xfe\x01\n\x11ResourceEventType\x12\x1f\n\x1bRESOURCE_EVENT_TYPE_UNKNOWN\x10\x00\x12\x1f\n\x1bRESOURCE_EVENT_TYPE_STARTED\x10\x01\x12!\n\x1dRESOURCE_EVENT_TYPE_COMPLETED\x10\x02\x12\x1e\n\x1aRESOURCE_EVENT_TYPE_FAILED\x10\x03\x12!\n\x1dRESOURCE_EVENT_TYPE_CANCELLED\x10\x04\x12!\n\x1dRESOURCE_EVENT_TYPE_TIMED_OUT\x10\x05\x12\x1e\n\x1aRESOURCE_EVENT_TYPE_STREAM\x10\x06*<\n\x14WorkflowRunEventType\x12$\n WORKFLOW_RUN_EVENT_TYPE_FINISHED\x10\x00\x32\xf8\x06\n\nDispatcher\x12=\n\x08Register\x12\x16.WorkerRegisterRequest\x1a\x17.WorkerRegisterResponse\"\x00\x12\x33\n\x06Listen\x12\x14.WorkerListenRequest\x1a\x0f.AssignedAction\"\x00\x30\x01\x12\x35\n\x08ListenV2\x12\x14.WorkerListenRequest\x1a\x0f.AssignedAction\"\x00\x30\x01\x12\x34\n\tHeartbeat\x12\x11.HeartbeatRequest\x1a\x12.HeartbeatResponse\"\x00\x12R\n\x19SubscribeToWorkflowEvents\x12!.SubscribeToWorkflowEventsRequest\x1a\x0e.WorkflowEvent\"\x00\x30\x01\x12S\n\x17SubscribeToWorkflowRuns\x12\x1f.SubscribeToWorkflowRunsRequest\x1a\x11.WorkflowRunEvent\"\x00(\x01\x30\x01\x12?\n\x13SendStepActionEvent\x12\x10.StepActionEvent\x1a\x14.ActionEventResponse\"\x00\x12G\n\x17SendGroupKeyActionEvent\x12\x14.GroupKeyActionEvent\x1a\x14.ActionEventResponse\"\x00\x12<\n\x10PutOverridesData\x12\x0e.OverridesData\x1a\x16.OverridesDataResponse\"\x00\x12\x46\n\x0bUnsubscribe\x12\x19.WorkerUnsubscribeRequest\x1a\x1a.WorkerUnsubscribeResponse\"\x00\x12\x43\n\x0eRefreshTimeout\x12\x16.RefreshTimeoutRequest\x1a\x17.RefreshTimeoutResponse\"\x00\x12:\n\x0bReleaseSlot\x12\x13.ReleaseSlotRequest\x1a\x14.ReleaseSlotResponse\"\x00\x12O\n\x12UpsertWorkerLabels\x12\x1a.UpsertWorkerLabelsRequest\x1a\x1b.UpsertWorkerLabelsResponse\"\x00\x42GZEgithub.com/hatchet-dev/hatchet/internal/services/dispatcher/contractsb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'dispatcher_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  _globals['DESCRIPTOR']._options = None
  _globals['DESCRIPTOR']._serialized_options = b'ZEgithub.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts'
  _globals['_WORKERREGISTERREQUEST_LABELSENTRY']._options = None
  _globals['_WORKERREGISTERREQUEST_LABELSENTRY']._serialized_options = b'8\001'
  _globals['_UPSERTWORKERLABELSREQUEST_LABELSENTRY']._options = None
  _globals['_UPSERTWORKERLABELSREQUEST_LABELSENTRY']._serialized_options = b'8\001'
  _globals['_ACTIONTYPE']._serialized_start=3225
  _globals['_ACTIONTYPE']._serialized_end=3303
  _globals['_GROUPKEYACTIONEVENTTYPE']._serialized_start=3306
  _globals['_GROUPKEYACTIONEVENTTYPE']._serialized_end=3468
  _globals['_STEPACTIONEVENTTYPE']._serialized_start=3471
  _globals['_STEPACTIONEVENTTYPE']._serialized_end=3673
  _globals['_RESOURCETYPE']._serialized_start=3675
  _globals['_RESOURCETYPE']._serialized_end=3776
  _globals['_RESOURCEEVENTTYPE']._serialized_start=3779
  _globals['_RESOURCEEVENTTYPE']._serialized_end=4033
  _globals['_WORKFLOWRUNEVENTTYPE']._serialized_start=4035
  _globals['_WORKFLOWRUNEVENTTYPE']._serialized_end=4095
  _globals['_WORKERLABELS']._serialized_start=53
  _globals['_WORKERLABELS']._serialized_end=139
  _globals['_WORKERREGISTERREQUEST']._serialized_start=142
  _globals['_WORKERREGISTERREQUEST']._serialized_end=406
  _globals['_WORKERREGISTERREQUEST_LABELSENTRY']._serialized_start=320
  _globals['_WORKERREGISTERREQUEST_LABELSENTRY']._serialized_end=380
  _globals['_WORKERREGISTERRESPONSE']._serialized_start=408
  _globals['_WORKERREGISTERRESPONSE']._serialized_end=488
  _globals['_UPSERTWORKERLABELSREQUEST']._serialized_start=491
  _globals['_UPSERTWORKERLABELSREQUEST']._serialized_end=654
  _globals['_UPSERTWORKERLABELSREQUEST_LABELSENTRY']._serialized_start=320
  _globals['_UPSERTWORKERLABELSREQUEST_LABELSENTRY']._serialized_end=380
  _globals['_UPSERTWORKERLABELSRESPONSE']._serialized_start=656
  _globals['_UPSERTWORKERLABELSRESPONSE']._serialized_end=720
  _globals['_ASSIGNEDACTION']._serialized_start=723
  _globals['_ASSIGNEDACTION']._serialized_end=1241
  _globals['_WORKERLISTENREQUEST']._serialized_start=1243
  _globals['_WORKERLISTENREQUEST']._serialized_end=1282
  _globals['_WORKERUNSUBSCRIBEREQUEST']._serialized_start=1284
  _globals['_WORKERUNSUBSCRIBEREQUEST']._serialized_end=1328
  _globals['_WORKERUNSUBSCRIBERESPONSE']._serialized_start=1330
  _globals['_WORKERUNSUBSCRIBERESPONSE']._serialized_end=1393
  _globals['_GROUPKEYACTIONEVENT']._serialized_start=1396
  _globals['_GROUPKEYACTIONEVENT']._serialized_end=1621
  _globals['_STEPACTIONEVENT']._serialized_start=1624
  _globals['_STEPACTIONEVENT']._serialized_end=1860
  _globals['_ACTIONEVENTRESPONSE']._serialized_start=1862
  _globals['_ACTIONEVENTRESPONSE']._serialized_end=1919
  _globals['_SUBSCRIBETOWORKFLOWEVENTSREQUEST']._serialized_start=1922
  _globals['_SUBSCRIBETOWORKFLOWEVENTSREQUEST']._serialized_end=2114
  _globals['_SUBSCRIBETOWORKFLOWRUNSREQUEST']._serialized_start=2116
  _globals['_SUBSCRIBETOWORKFLOWRUNSREQUEST']._serialized_end=2171
  _globals['_WORKFLOWEVENT']._serialized_start=2174
  _globals['_WORKFLOWEVENT']._serialized_end=2480
  _globals['_WORKFLOWRUNEVENT']._serialized_start=2483
  _globals['_WORKFLOWRUNEVENT']._serialized_end=2651
  _globals['_STEPRUNRESULT']._serialized_start=2654
  _globals['_STEPRUNRESULT']._serialized_end=2792
  _globals['_OVERRIDESDATA']._serialized_start=2794
  _globals['_OVERRIDESDATA']._serialized_end=2881
  _globals['_OVERRIDESDATARESPONSE']._serialized_start=2883
  _globals['_OVERRIDESDATARESPONSE']._serialized_end=2906
  _globals['_HEARTBEATREQUEST']._serialized_start=2908
  _globals['_HEARTBEATREQUEST']._serialized_end=2993
  _globals['_HEARTBEATRESPONSE']._serialized_start=2995
  _globals['_HEARTBEATRESPONSE']._serialized_end=3014
  _globals['_REFRESHTIMEOUTREQUEST']._serialized_start=3016
  _globals['_REFRESHTIMEOUTREQUEST']._serialized_end=3086
  _globals['_REFRESHTIMEOUTRESPONSE']._serialized_start=3088
  _globals['_REFRESHTIMEOUTRESPONSE']._serialized_end=3159
  _globals['_RELEASESLOTREQUEST']._serialized_start=3161
  _globals['_RELEASESLOTREQUEST']._serialized_end=3200
  _globals['_RELEASESLOTRESPONSE']._serialized_start=3202
  _globals['_RELEASESLOTRESPONSE']._serialized_end=3223
  _globals['_DISPATCHER']._serialized_start=4098
  _globals['_DISPATCHER']._serialized_end=4986
# @@protoc_insertion_point(module_scope)
