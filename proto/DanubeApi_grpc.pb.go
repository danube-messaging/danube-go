// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v3.21.12
// source: DanubeApi.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	ProducerService_CreateProducer_FullMethodName = "/danube.ProducerService/CreateProducer"
	ProducerService_SendMessage_FullMethodName    = "/danube.ProducerService/SendMessage"
)

// ProducerServiceClient is the client API for ProducerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ProducerServiceClient interface {
	// Creates a new Producer on a topic
	CreateProducer(ctx context.Context, in *ProducerRequest, opts ...grpc.CallOption) (*ProducerResponse, error)
	// Sends a message from the Producer
	SendMessage(ctx context.Context, in *StreamMessage, opts ...grpc.CallOption) (*MessageResponse, error)
}

type producerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewProducerServiceClient(cc grpc.ClientConnInterface) ProducerServiceClient {
	return &producerServiceClient{cc}
}

func (c *producerServiceClient) CreateProducer(ctx context.Context, in *ProducerRequest, opts ...grpc.CallOption) (*ProducerResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ProducerResponse)
	err := c.cc.Invoke(ctx, ProducerService_CreateProducer_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *producerServiceClient) SendMessage(ctx context.Context, in *StreamMessage, opts ...grpc.CallOption) (*MessageResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(MessageResponse)
	err := c.cc.Invoke(ctx, ProducerService_SendMessage_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ProducerServiceServer is the server API for ProducerService service.
// All implementations must embed UnimplementedProducerServiceServer
// for forward compatibility.
type ProducerServiceServer interface {
	// Creates a new Producer on a topic
	CreateProducer(context.Context, *ProducerRequest) (*ProducerResponse, error)
	// Sends a message from the Producer
	SendMessage(context.Context, *StreamMessage) (*MessageResponse, error)
	mustEmbedUnimplementedProducerServiceServer()
}

// UnimplementedProducerServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedProducerServiceServer struct{}

func (UnimplementedProducerServiceServer) CreateProducer(context.Context, *ProducerRequest) (*ProducerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateProducer not implemented")
}
func (UnimplementedProducerServiceServer) SendMessage(context.Context, *StreamMessage) (*MessageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SendMessage not implemented")
}
func (UnimplementedProducerServiceServer) mustEmbedUnimplementedProducerServiceServer() {}
func (UnimplementedProducerServiceServer) testEmbeddedByValue()                         {}

// UnsafeProducerServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ProducerServiceServer will
// result in compilation errors.
type UnsafeProducerServiceServer interface {
	mustEmbedUnimplementedProducerServiceServer()
}

func RegisterProducerServiceServer(s grpc.ServiceRegistrar, srv ProducerServiceServer) {
	// If the following call pancis, it indicates UnimplementedProducerServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&ProducerService_ServiceDesc, srv)
}

func _ProducerService_CreateProducer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProducerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProducerServiceServer).CreateProducer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ProducerService_CreateProducer_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProducerServiceServer).CreateProducer(ctx, req.(*ProducerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProducerService_SendMessage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StreamMessage)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProducerServiceServer).SendMessage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ProducerService_SendMessage_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProducerServiceServer).SendMessage(ctx, req.(*StreamMessage))
	}
	return interceptor(ctx, in, info, handler)
}

// ProducerService_ServiceDesc is the grpc.ServiceDesc for ProducerService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ProducerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "danube.ProducerService",
	HandlerType: (*ProducerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateProducer",
			Handler:    _ProducerService_CreateProducer_Handler,
		},
		{
			MethodName: "SendMessage",
			Handler:    _ProducerService_SendMessage_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "DanubeApi.proto",
}

const (
	ConsumerService_Subscribe_FullMethodName       = "/danube.ConsumerService/Subscribe"
	ConsumerService_ReceiveMessages_FullMethodName = "/danube.ConsumerService/ReceiveMessages"
	ConsumerService_Ack_FullMethodName             = "/danube.ConsumerService/Ack"
)

// ConsumerServiceClient is the client API for ConsumerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ConsumerServiceClient interface {
	// Creates a new Subscriber
	Subscribe(ctx context.Context, in *ConsumerRequest, opts ...grpc.CallOption) (*ConsumerResponse, error)
	// Streaming messages to the Subscriber
	ReceiveMessages(ctx context.Context, in *ReceiveRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[StreamMessage], error)
	// Acknowledges receipt of a message from the Consumer
	Ack(ctx context.Context, in *AckRequest, opts ...grpc.CallOption) (*AckResponse, error)
}

type consumerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewConsumerServiceClient(cc grpc.ClientConnInterface) ConsumerServiceClient {
	return &consumerServiceClient{cc}
}

func (c *consumerServiceClient) Subscribe(ctx context.Context, in *ConsumerRequest, opts ...grpc.CallOption) (*ConsumerResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ConsumerResponse)
	err := c.cc.Invoke(ctx, ConsumerService_Subscribe_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *consumerServiceClient) ReceiveMessages(ctx context.Context, in *ReceiveRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[StreamMessage], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &ConsumerService_ServiceDesc.Streams[0], ConsumerService_ReceiveMessages_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[ReceiveRequest, StreamMessage]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type ConsumerService_ReceiveMessagesClient = grpc.ServerStreamingClient[StreamMessage]

func (c *consumerServiceClient) Ack(ctx context.Context, in *AckRequest, opts ...grpc.CallOption) (*AckResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AckResponse)
	err := c.cc.Invoke(ctx, ConsumerService_Ack_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ConsumerServiceServer is the server API for ConsumerService service.
// All implementations must embed UnimplementedConsumerServiceServer
// for forward compatibility.
type ConsumerServiceServer interface {
	// Creates a new Subscriber
	Subscribe(context.Context, *ConsumerRequest) (*ConsumerResponse, error)
	// Streaming messages to the Subscriber
	ReceiveMessages(*ReceiveRequest, grpc.ServerStreamingServer[StreamMessage]) error
	// Acknowledges receipt of a message from the Consumer
	Ack(context.Context, *AckRequest) (*AckResponse, error)
	mustEmbedUnimplementedConsumerServiceServer()
}

// UnimplementedConsumerServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedConsumerServiceServer struct{}

func (UnimplementedConsumerServiceServer) Subscribe(context.Context, *ConsumerRequest) (*ConsumerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Subscribe not implemented")
}
func (UnimplementedConsumerServiceServer) ReceiveMessages(*ReceiveRequest, grpc.ServerStreamingServer[StreamMessage]) error {
	return status.Errorf(codes.Unimplemented, "method ReceiveMessages not implemented")
}
func (UnimplementedConsumerServiceServer) Ack(context.Context, *AckRequest) (*AckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ack not implemented")
}
func (UnimplementedConsumerServiceServer) mustEmbedUnimplementedConsumerServiceServer() {}
func (UnimplementedConsumerServiceServer) testEmbeddedByValue()                         {}

// UnsafeConsumerServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ConsumerServiceServer will
// result in compilation errors.
type UnsafeConsumerServiceServer interface {
	mustEmbedUnimplementedConsumerServiceServer()
}

func RegisterConsumerServiceServer(s grpc.ServiceRegistrar, srv ConsumerServiceServer) {
	// If the following call pancis, it indicates UnimplementedConsumerServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&ConsumerService_ServiceDesc, srv)
}

func _ConsumerService_Subscribe_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConsumerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ConsumerServiceServer).Subscribe(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ConsumerService_Subscribe_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ConsumerServiceServer).Subscribe(ctx, req.(*ConsumerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ConsumerService_ReceiveMessages_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ReceiveRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ConsumerServiceServer).ReceiveMessages(m, &grpc.GenericServerStream[ReceiveRequest, StreamMessage]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type ConsumerService_ReceiveMessagesServer = grpc.ServerStreamingServer[StreamMessage]

func _ConsumerService_Ack_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AckRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ConsumerServiceServer).Ack(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ConsumerService_Ack_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ConsumerServiceServer).Ack(ctx, req.(*AckRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ConsumerService_ServiceDesc is the grpc.ServiceDesc for ConsumerService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ConsumerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "danube.ConsumerService",
	HandlerType: (*ConsumerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Subscribe",
			Handler:    _ConsumerService_Subscribe_Handler,
		},
		{
			MethodName: "Ack",
			Handler:    _ConsumerService_Ack_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ReceiveMessages",
			Handler:       _ConsumerService_ReceiveMessages_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "DanubeApi.proto",
}

const (
	Discovery_TopicLookup_FullMethodName     = "/danube.Discovery/TopicLookup"
	Discovery_TopicPartitions_FullMethodName = "/danube.Discovery/TopicPartitions"
	Discovery_GetSchema_FullMethodName       = "/danube.Discovery/GetSchema"
)

// DiscoveryClient is the client API for Discovery service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DiscoveryClient interface {
	// Query the Danube broker for information about a specific topic.
	// returns metadata about the topic, including the broker(s) responsible for it.
	TopicLookup(ctx context.Context, in *TopicLookupRequest, opts ...grpc.CallOption) (*TopicLookupResponse, error)
	// Query the Danube broker for information about topic partitions.
	// returns an array with the topic partitions names
	TopicPartitions(ctx context.Context, in *TopicLookupRequest, opts ...grpc.CallOption) (*TopicPartitionsResponse, error)
	// Get the schema associated with the topic
	GetSchema(ctx context.Context, in *SchemaRequest, opts ...grpc.CallOption) (*SchemaResponse, error)
}

type discoveryClient struct {
	cc grpc.ClientConnInterface
}

func NewDiscoveryClient(cc grpc.ClientConnInterface) DiscoveryClient {
	return &discoveryClient{cc}
}

func (c *discoveryClient) TopicLookup(ctx context.Context, in *TopicLookupRequest, opts ...grpc.CallOption) (*TopicLookupResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(TopicLookupResponse)
	err := c.cc.Invoke(ctx, Discovery_TopicLookup_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *discoveryClient) TopicPartitions(ctx context.Context, in *TopicLookupRequest, opts ...grpc.CallOption) (*TopicPartitionsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(TopicPartitionsResponse)
	err := c.cc.Invoke(ctx, Discovery_TopicPartitions_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *discoveryClient) GetSchema(ctx context.Context, in *SchemaRequest, opts ...grpc.CallOption) (*SchemaResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(SchemaResponse)
	err := c.cc.Invoke(ctx, Discovery_GetSchema_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DiscoveryServer is the server API for Discovery service.
// All implementations must embed UnimplementedDiscoveryServer
// for forward compatibility.
type DiscoveryServer interface {
	// Query the Danube broker for information about a specific topic.
	// returns metadata about the topic, including the broker(s) responsible for it.
	TopicLookup(context.Context, *TopicLookupRequest) (*TopicLookupResponse, error)
	// Query the Danube broker for information about topic partitions.
	// returns an array with the topic partitions names
	TopicPartitions(context.Context, *TopicLookupRequest) (*TopicPartitionsResponse, error)
	// Get the schema associated with the topic
	GetSchema(context.Context, *SchemaRequest) (*SchemaResponse, error)
	mustEmbedUnimplementedDiscoveryServer()
}

// UnimplementedDiscoveryServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedDiscoveryServer struct{}

func (UnimplementedDiscoveryServer) TopicLookup(context.Context, *TopicLookupRequest) (*TopicLookupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TopicLookup not implemented")
}
func (UnimplementedDiscoveryServer) TopicPartitions(context.Context, *TopicLookupRequest) (*TopicPartitionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TopicPartitions not implemented")
}
func (UnimplementedDiscoveryServer) GetSchema(context.Context, *SchemaRequest) (*SchemaResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetSchema not implemented")
}
func (UnimplementedDiscoveryServer) mustEmbedUnimplementedDiscoveryServer() {}
func (UnimplementedDiscoveryServer) testEmbeddedByValue()                   {}

// UnsafeDiscoveryServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DiscoveryServer will
// result in compilation errors.
type UnsafeDiscoveryServer interface {
	mustEmbedUnimplementedDiscoveryServer()
}

func RegisterDiscoveryServer(s grpc.ServiceRegistrar, srv DiscoveryServer) {
	// If the following call pancis, it indicates UnimplementedDiscoveryServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&Discovery_ServiceDesc, srv)
}

func _Discovery_TopicLookup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TopicLookupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).TopicLookup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Discovery_TopicLookup_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).TopicLookup(ctx, req.(*TopicLookupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_TopicPartitions_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TopicLookupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).TopicPartitions(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Discovery_TopicPartitions_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).TopicPartitions(ctx, req.(*TopicLookupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_GetSchema_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SchemaRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).GetSchema(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Discovery_GetSchema_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).GetSchema(ctx, req.(*SchemaRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Discovery_ServiceDesc is the grpc.ServiceDesc for Discovery service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Discovery_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "danube.Discovery",
	HandlerType: (*DiscoveryServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "TopicLookup",
			Handler:    _Discovery_TopicLookup_Handler,
		},
		{
			MethodName: "TopicPartitions",
			Handler:    _Discovery_TopicPartitions_Handler,
		},
		{
			MethodName: "GetSchema",
			Handler:    _Discovery_GetSchema_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "DanubeApi.proto",
}

const (
	HealthCheck_HealthCheck_FullMethodName = "/danube.HealthCheck/HealthCheck"
)

// HealthCheckClient is the client API for HealthCheck service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type HealthCheckClient interface {
	HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error)
}

type healthCheckClient struct {
	cc grpc.ClientConnInterface
}

func NewHealthCheckClient(cc grpc.ClientConnInterface) HealthCheckClient {
	return &healthCheckClient{cc}
}

func (c *healthCheckClient) HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(HealthCheckResponse)
	err := c.cc.Invoke(ctx, HealthCheck_HealthCheck_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// HealthCheckServer is the server API for HealthCheck service.
// All implementations must embed UnimplementedHealthCheckServer
// for forward compatibility.
type HealthCheckServer interface {
	HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error)
	mustEmbedUnimplementedHealthCheckServer()
}

// UnimplementedHealthCheckServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedHealthCheckServer struct{}

func (UnimplementedHealthCheckServer) HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HealthCheck not implemented")
}
func (UnimplementedHealthCheckServer) mustEmbedUnimplementedHealthCheckServer() {}
func (UnimplementedHealthCheckServer) testEmbeddedByValue()                     {}

// UnsafeHealthCheckServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to HealthCheckServer will
// result in compilation errors.
type UnsafeHealthCheckServer interface {
	mustEmbedUnimplementedHealthCheckServer()
}

func RegisterHealthCheckServer(s grpc.ServiceRegistrar, srv HealthCheckServer) {
	// If the following call pancis, it indicates UnimplementedHealthCheckServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&HealthCheck_ServiceDesc, srv)
}

func _HealthCheck_HealthCheck_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HealthCheckRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(HealthCheckServer).HealthCheck(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: HealthCheck_HealthCheck_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(HealthCheckServer).HealthCheck(ctx, req.(*HealthCheckRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// HealthCheck_ServiceDesc is the grpc.ServiceDesc for HealthCheck service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var HealthCheck_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "danube.HealthCheck",
	HandlerType: (*HealthCheckServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "HealthCheck",
			Handler:    _HealthCheck_HealthCheck_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "DanubeApi.proto",
}
