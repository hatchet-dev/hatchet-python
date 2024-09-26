# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: events.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0c\x65vents.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"\xb4\x01\n\x05\x45vent\x12\x10\n\x08tenantId\x18\x01 \x01(\t\x12\x0f\n\x07\x65ventId\x18\x02 \x01(\t\x12\x0b\n\x03key\x18\x03 \x01(\t\x12\x0f\n\x07payload\x18\x04 \x01(\t\x12\x32\n\x0e\x65ventTimestamp\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x1f\n\x12\x61\x64\x64itionalMetadata\x18\x06 \x01(\tH\x00\x88\x01\x01\x42\x15\n\x13_additionalMetadata\" \n\x06\x45vents\x12\x16\n\x06\x65vents\x18\x01 \x03(\x0b\x32\x06.Event\"\x92\x01\n\rPutLogRequest\x12\x11\n\tstepRunId\x18\x01 \x01(\t\x12-\n\tcreatedAt\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0f\n\x07message\x18\x03 \x01(\t\x12\x12\n\x05level\x18\x04 \x01(\tH\x00\x88\x01\x01\x12\x10\n\x08metadata\x18\x05 \x01(\tB\x08\n\x06_level\"\x10\n\x0ePutLogResponse\"|\n\x15PutStreamEventRequest\x12\x11\n\tstepRunId\x18\x01 \x01(\t\x12-\n\tcreatedAt\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0f\n\x07message\x18\x03 \x01(\x0c\x12\x10\n\x08metadata\x18\x05 \x01(\t\"\x18\n\x16PutStreamEventResponse\"9\n\x14\x42ulkPushEventRequest\x12!\n\x06\x65vents\x18\x01 \x03(\x0b\x32\x11.PushEventRequest\"\x9c\x01\n\x10PushEventRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x0f\n\x07payload\x18\x02 \x01(\t\x12\x32\n\x0e\x65ventTimestamp\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x1f\n\x12\x61\x64\x64itionalMetadata\x18\x04 \x01(\tH\x00\x88\x01\x01\x42\x15\n\x13_additionalMetadata\"%\n\x12ReplayEventRequest\x12\x0f\n\x07\x65ventId\x18\x01 \x01(\t2\x88\x02\n\rEventsService\x12#\n\x04Push\x12\x11.PushEventRequest\x1a\x06.Event\"\x00\x12,\n\x08\x42ulkPush\x12\x15.BulkPushEventRequest\x1a\x07.Events\"\x00\x12\x32\n\x11ReplaySingleEvent\x12\x13.ReplayEventRequest\x1a\x06.Event\"\x00\x12+\n\x06PutLog\x12\x0e.PutLogRequest\x1a\x0f.PutLogResponse\"\x00\x12\x43\n\x0ePutStreamEvent\x12\x16.PutStreamEventRequest\x1a\x17.PutStreamEventResponse\"\x00\x42GZEgithub.com/hatchet-dev/hatchet/internal/services/dispatcher/contractsb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'events_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  _globals['DESCRIPTOR']._options = None
  _globals['DESCRIPTOR']._serialized_options = b'ZEgithub.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts'
  _globals['_EVENT']._serialized_start=50
  _globals['_EVENT']._serialized_end=230
  _globals['_EVENTS']._serialized_start=232
  _globals['_EVENTS']._serialized_end=264
  _globals['_PUTLOGREQUEST']._serialized_start=267
  _globals['_PUTLOGREQUEST']._serialized_end=413
  _globals['_PUTLOGRESPONSE']._serialized_start=415
  _globals['_PUTLOGRESPONSE']._serialized_end=431
  _globals['_PUTSTREAMEVENTREQUEST']._serialized_start=433
  _globals['_PUTSTREAMEVENTREQUEST']._serialized_end=557
  _globals['_PUTSTREAMEVENTRESPONSE']._serialized_start=559
  _globals['_PUTSTREAMEVENTRESPONSE']._serialized_end=583
  _globals['_BULKPUSHEVENTREQUEST']._serialized_start=585
  _globals['_BULKPUSHEVENTREQUEST']._serialized_end=642
  _globals['_PUSHEVENTREQUEST']._serialized_start=645
  _globals['_PUSHEVENTREQUEST']._serialized_end=801
  _globals['_REPLAYEVENTREQUEST']._serialized_start=803
  _globals['_REPLAYEVENTREQUEST']._serialized_end=840
  _globals['_EVENTSSERVICE']._serialized_start=843
  _globals['_EVENTSSERVICE']._serialized_end=1107
# @@protoc_insertion_point(module_scope)
