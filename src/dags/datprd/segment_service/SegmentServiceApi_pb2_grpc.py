# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import dags.datprd.segment_service.SegmentServiceApi_pb2 as SegmentServiceApi__pb2

GRPC_GENERATED_VERSION = '1.70.0'
GRPC_VERSION = grpc.__version__
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    raise RuntimeError(
        f'The grpc package installed is at version {GRPC_VERSION},' +
        ' but the generated code in SegmentServiceApi_pb2_grpc.py depends on' + f' grpcio>={GRPC_GENERATED_VERSION}.' +
        f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}' +
        f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
    )


class SegmentServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Ping = channel.unary_unary(
            '/segment.api.SegmentService/Ping',
            request_serializer=SegmentServiceApi__pb2.PingRequest.SerializeToString,
            response_deserializer=SegmentServiceApi__pb2.PingReply.FromString,
            _registered_method=True
        )
        self.GetReceivedSegment = channel.unary_stream(
            '/segment.api.SegmentService/GetReceivedSegment',
            request_serializer=SegmentServiceApi__pb2.GetReceivedSegmentRequest.SerializeToString,
            response_deserializer=SegmentServiceApi__pb2.GetReceivedSegmentReply.FromString,
            _registered_method=True
        )
        self.GetActiveSegment = channel.unary_stream(
            '/segment.api.SegmentService/GetActiveSegment',
            request_serializer=SegmentServiceApi__pb2.GetActiveSegmentRequest.SerializeToString,
            response_deserializer=SegmentServiceApi__pb2.GetActiveSegmentReply.FromString,
            _registered_method=True
        )
        self.GetActiveAudience = channel.unary_stream(
            '/segment.api.SegmentService/GetActiveAudience',
            request_serializer=SegmentServiceApi__pb2.GetActiveAudienceRequest.SerializeToString,
            response_deserializer=SegmentServiceApi__pb2.GetActiveAudienceReply.FromString,
            _registered_method=True
        )
        self.ExportActiveAudiences = channel.unary_unary(
            '/segment.api.SegmentService/ExportActiveAudiences',
            request_serializer=SegmentServiceApi__pb2.ExportActiveAudiencesRequest.SerializeToString,
            response_deserializer=SegmentServiceApi__pb2.ExportActiveAudiencesReply.FromString,
            _registered_method=True
        )
        self.GetExportActiveAudiencesStatus = channel.unary_unary(
            '/segment.api.SegmentService/GetExportActiveAudiencesStatus',
            request_serializer=SegmentServiceApi__pb2.GetExportActiveAudiencesStatusRequest.SerializeToString,
            response_deserializer=SegmentServiceApi__pb2.GetExportActiveAudiencesStatusReply.FromString,
            _registered_method=True
        )
        self.GetPersonReceivedSegment = channel.unary_stream(
            '/segment.api.SegmentService/GetPersonReceivedSegment',
            request_serializer=SegmentServiceApi__pb2.GetPersonReceivedSegmentRequest.SerializeToString,
            response_deserializer=SegmentServiceApi__pb2.GetPersonReceivedSegmentReply.FromString,
            _registered_method=True
        )


class SegmentServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Ping(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetReceivedSegment(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetActiveSegment(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetActiveAudience(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ExportActiveAudiences(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetExportActiveAudiencesStatus(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetPersonReceivedSegment(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_SegmentServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        'Ping':
        grpc.unary_unary_rpc_method_handler(
            servicer.Ping,
            request_deserializer=SegmentServiceApi__pb2.PingRequest.FromString,
            response_serializer=SegmentServiceApi__pb2.PingReply.SerializeToString,
        ),
        'GetReceivedSegment':
        grpc.unary_stream_rpc_method_handler(
            servicer.GetReceivedSegment,
            request_deserializer=SegmentServiceApi__pb2.GetReceivedSegmentRequest.FromString,
            response_serializer=SegmentServiceApi__pb2.GetReceivedSegmentReply.SerializeToString,
        ),
        'GetActiveSegment':
        grpc.unary_stream_rpc_method_handler(
            servicer.GetActiveSegment,
            request_deserializer=SegmentServiceApi__pb2.GetActiveSegmentRequest.FromString,
            response_serializer=SegmentServiceApi__pb2.GetActiveSegmentReply.SerializeToString,
        ),
        'GetActiveAudience':
        grpc.unary_stream_rpc_method_handler(
            servicer.GetActiveAudience,
            request_deserializer=SegmentServiceApi__pb2.GetActiveAudienceRequest.FromString,
            response_serializer=SegmentServiceApi__pb2.GetActiveAudienceReply.SerializeToString,
        ),
        'ExportActiveAudiences':
        grpc.unary_unary_rpc_method_handler(
            servicer.ExportActiveAudiences,
            request_deserializer=SegmentServiceApi__pb2.ExportActiveAudiencesRequest.FromString,
            response_serializer=SegmentServiceApi__pb2.ExportActiveAudiencesReply.SerializeToString,
        ),
        'GetExportActiveAudiencesStatus':
        grpc.unary_unary_rpc_method_handler(
            servicer.GetExportActiveAudiencesStatus,
            request_deserializer=SegmentServiceApi__pb2.GetExportActiveAudiencesStatusRequest.FromString,
            response_serializer=SegmentServiceApi__pb2.GetExportActiveAudiencesStatusReply.SerializeToString,
        ),
        'GetPersonReceivedSegment':
        grpc.unary_stream_rpc_method_handler(
            servicer.GetPersonReceivedSegment,
            request_deserializer=SegmentServiceApi__pb2.GetPersonReceivedSegmentRequest.FromString,
            response_serializer=SegmentServiceApi__pb2.GetPersonReceivedSegmentReply.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler('segment.api.SegmentService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler, ))
    server.add_registered_method_handlers('segment.api.SegmentService', rpc_method_handlers)


# This class is part of an EXPERIMENTAL API.
class SegmentService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Ping(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/segment.api.SegmentService/Ping',
            SegmentServiceApi__pb2.PingRequest.SerializeToString,
            SegmentServiceApi__pb2.PingReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True
        )

    @staticmethod
    def GetReceivedSegment(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            '/segment.api.SegmentService/GetReceivedSegment',
            SegmentServiceApi__pb2.GetReceivedSegmentRequest.SerializeToString,
            SegmentServiceApi__pb2.GetReceivedSegmentReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True
        )

    @staticmethod
    def GetActiveSegment(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            '/segment.api.SegmentService/GetActiveSegment',
            SegmentServiceApi__pb2.GetActiveSegmentRequest.SerializeToString,
            SegmentServiceApi__pb2.GetActiveSegmentReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True
        )

    @staticmethod
    def GetActiveAudience(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            '/segment.api.SegmentService/GetActiveAudience',
            SegmentServiceApi__pb2.GetActiveAudienceRequest.SerializeToString,
            SegmentServiceApi__pb2.GetActiveAudienceReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True
        )

    @staticmethod
    def ExportActiveAudiences(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/segment.api.SegmentService/ExportActiveAudiences',
            SegmentServiceApi__pb2.ExportActiveAudiencesRequest.SerializeToString,
            SegmentServiceApi__pb2.ExportActiveAudiencesReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True
        )

    @staticmethod
    def GetExportActiveAudiencesStatus(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/segment.api.SegmentService/GetExportActiveAudiencesStatus',
            SegmentServiceApi__pb2.GetExportActiveAudiencesStatusRequest.SerializeToString,
            SegmentServiceApi__pb2.GetExportActiveAudiencesStatusReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True
        )

    @staticmethod
    def GetPersonReceivedSegment(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            '/segment.api.SegmentService/GetPersonReceivedSegment',
            SegmentServiceApi__pb2.GetPersonReceivedSegmentRequest.SerializeToString,
            SegmentServiceApi__pb2.GetPersonReceivedSegmentReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True
        )
