// Code generated by protoc-gen-go-syncs. DO NOT EDIT.
// versions:
// - protoc-gen-go-syncs v1.2.0
// - protoc             v3.21.12
// source: proto/warehouse/warehouse.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// WarehouseClient is the client API for Warehouse service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type WarehouseClient interface {
	GetHealth(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*wrapperspb.BoolValue, error)
	GetWHUploads(ctx context.Context, in *WHUploadsRequest, opts ...grpc.CallOption) (*WHUploadsResponse, error)
	GetWHUpload(ctx context.Context, in *WHUploadRequest, opts ...grpc.CallOption) (*WHUploadResponse, error)
	TriggerWHUpload(ctx context.Context, in *WHUploadRequest, opts ...grpc.CallOption) (*TriggerWhUploadsResponse, error)
	TriggerWHUploads(ctx context.Context, in *WHUploadsRequest, opts ...grpc.CallOption) (*TriggerWhUploadsResponse, error)
	Validate(ctx context.Context, in *WHValidationRequest, opts ...grpc.CallOption) (*WHValidationResponse, error)
	RetryWHUploads(ctx context.Context, in *RetryWHUploadsRequest, opts ...grpc.CallOption) (*RetryWHUploadsResponse, error)
	ValidateObjectStorageDestination(ctx context.Context, in *ValidateObjectStorageRequest, opts ...grpc.CallOption) (*ValidateObjectStorageResponse, error)
	CountWHUploadsToRetry(ctx context.Context, in *RetryWHUploadsRequest, opts ...grpc.CallOption) (*RetryWHUploadsResponse, error)
}

type warehouseClient struct {
	cc grpc.ClientConnInterface
}

func NewWarehouseClient(cc grpc.ClientConnInterface) WarehouseClient {
	return &warehouseClient{cc}
}

func (c *warehouseClient) GetHealth(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*wrapperspb.BoolValue, error) {
	out := new(wrapperspb.BoolValue)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/GetHealth", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *warehouseClient) GetWHUploads(ctx context.Context, in *WHUploadsRequest, opts ...grpc.CallOption) (*WHUploadsResponse, error) {
	out := new(WHUploadsResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/GetWHUploads", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *warehouseClient) GetWHUpload(ctx context.Context, in *WHUploadRequest, opts ...grpc.CallOption) (*WHUploadResponse, error) {
	out := new(WHUploadResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/GetWHUpload", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *warehouseClient) TriggerWHUpload(ctx context.Context, in *WHUploadRequest, opts ...grpc.CallOption) (*TriggerWhUploadsResponse, error) {
	out := new(TriggerWhUploadsResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/TriggerWHUpload", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *warehouseClient) TriggerWHUploads(ctx context.Context, in *WHUploadsRequest, opts ...grpc.CallOption) (*TriggerWhUploadsResponse, error) {
	out := new(TriggerWhUploadsResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/TriggerWHUploads", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *warehouseClient) Validate(ctx context.Context, in *WHValidationRequest, opts ...grpc.CallOption) (*WHValidationResponse, error) {
	out := new(WHValidationResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/Validate", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *warehouseClient) RetryWHUploads(ctx context.Context, in *RetryWHUploadsRequest, opts ...grpc.CallOption) (*RetryWHUploadsResponse, error) {
	out := new(RetryWHUploadsResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/RetryWHUploads", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *warehouseClient) ValidateObjectStorageDestination(ctx context.Context, in *ValidateObjectStorageRequest, opts ...grpc.CallOption) (*ValidateObjectStorageResponse, error) {
	out := new(ValidateObjectStorageResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/ValidateObjectStorageDestination", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *warehouseClient) CountWHUploadsToRetry(ctx context.Context, in *RetryWHUploadsRequest, opts ...grpc.CallOption) (*RetryWHUploadsResponse, error) {
	out := new(RetryWHUploadsResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/CountWHUploadsToRetry", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WarehouseServer is the server API for Warehouse service.
// All implementations must embed UnimplementedWarehouseServer
// for forward compatibility
type WarehouseServer interface {
	GetHealth(context.Context, *emptypb.Empty) (*wrapperspb.BoolValue, error)
	GetWHUploads(context.Context, *WHUploadsRequest) (*WHUploadsResponse, error)
	GetWHUpload(context.Context, *WHUploadRequest) (*WHUploadResponse, error)
	TriggerWHUpload(context.Context, *WHUploadRequest) (*TriggerWhUploadsResponse, error)
	TriggerWHUploads(context.Context, *WHUploadsRequest) (*TriggerWhUploadsResponse, error)
	Validate(context.Context, *WHValidationRequest) (*WHValidationResponse, error)
	RetryWHUploads(context.Context, *RetryWHUploadsRequest) (*RetryWHUploadsResponse, error)
	ValidateObjectStorageDestination(context.Context, *ValidateObjectStorageRequest) (*ValidateObjectStorageResponse, error)
	CountWHUploadsToRetry(context.Context, *RetryWHUploadsRequest) (*RetryWHUploadsResponse, error)
	mustEmbedUnimplementedWarehouseServer()
}

// UnimplementedWarehouseServer must be embedded to have forward compatible implementations.
type UnimplementedWarehouseServer struct {
}

func (UnimplementedWarehouseServer) GetHealth(context.Context, *emptypb.Empty) (*wrapperspb.BoolValue, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetHealth not implemented")
}
func (UnimplementedWarehouseServer) GetWHUploads(context.Context, *WHUploadsRequest) (*WHUploadsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWHUploads not implemented")
}
func (UnimplementedWarehouseServer) GetWHUpload(context.Context, *WHUploadRequest) (*WHUploadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWHUpload not implemented")
}
func (UnimplementedWarehouseServer) TriggerWHUpload(context.Context, *WHUploadRequest) (*TriggerWhUploadsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TriggerWHUpload not implemented")
}
func (UnimplementedWarehouseServer) TriggerWHUploads(context.Context, *WHUploadsRequest) (*TriggerWhUploadsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TriggerWHUploads not implemented")
}
func (UnimplementedWarehouseServer) Validate(context.Context, *WHValidationRequest) (*WHValidationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Validate not implemented")
}
func (UnimplementedWarehouseServer) RetryWHUploads(context.Context, *RetryWHUploadsRequest) (*RetryWHUploadsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RetryWHUploads not implemented")
}
func (UnimplementedWarehouseServer) ValidateObjectStorageDestination(context.Context, *ValidateObjectStorageRequest) (*ValidateObjectStorageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ValidateObjectStorageDestination not implemented")
}
func (UnimplementedWarehouseServer) CountWHUploadsToRetry(context.Context, *RetryWHUploadsRequest) (*RetryWHUploadsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CountWHUploadsToRetry not implemented")
}
func (UnimplementedWarehouseServer) mustEmbedUnimplementedWarehouseServer() {}

// UnsafeWarehouseServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to WarehouseServer will
// result in compilation errors.
type UnsafeWarehouseServer interface {
	mustEmbedUnimplementedWarehouseServer()
}

func RegisterWarehouseServer(s grpc.ServiceRegistrar, srv WarehouseServer) {
	s.RegisterService(&Warehouse_ServiceDesc, srv)
}

func _Warehouse_GetHealth_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WarehouseServer).GetHealth(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.Warehouse/GetHealth",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WarehouseServer).GetHealth(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Warehouse_GetWHUploads_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WHUploadsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WarehouseServer).GetWHUploads(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.Warehouse/GetWHUploads",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WarehouseServer).GetWHUploads(ctx, req.(*WHUploadsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Warehouse_GetWHUpload_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WHUploadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WarehouseServer).GetWHUpload(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.Warehouse/GetWHUpload",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WarehouseServer).GetWHUpload(ctx, req.(*WHUploadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Warehouse_TriggerWHUpload_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WHUploadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WarehouseServer).TriggerWHUpload(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.Warehouse/TriggerWHUpload",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WarehouseServer).TriggerWHUpload(ctx, req.(*WHUploadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Warehouse_TriggerWHUploads_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WHUploadsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WarehouseServer).TriggerWHUploads(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.Warehouse/TriggerWHUploads",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WarehouseServer).TriggerWHUploads(ctx, req.(*WHUploadsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Warehouse_Validate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WHValidationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WarehouseServer).Validate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.Warehouse/Validate",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WarehouseServer).Validate(ctx, req.(*WHValidationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Warehouse_RetryWHUploads_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RetryWHUploadsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WarehouseServer).RetryWHUploads(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.Warehouse/RetryWHUploads",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WarehouseServer).RetryWHUploads(ctx, req.(*RetryWHUploadsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Warehouse_ValidateObjectStorageDestination_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ValidateObjectStorageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WarehouseServer).ValidateObjectStorageDestination(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.Warehouse/ValidateObjectStorageDestination",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WarehouseServer).ValidateObjectStorageDestination(ctx, req.(*ValidateObjectStorageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Warehouse_CountWHUploadsToRetry_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RetryWHUploadsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WarehouseServer).CountWHUploadsToRetry(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.Warehouse/CountWHUploadsToRetry",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WarehouseServer).CountWHUploadsToRetry(ctx, req.(*RetryWHUploadsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Warehouse_ServiceDesc is the grpc.ServiceDesc for Warehouse service.
// It's only intended for direct use with syncs.RegisterService,
// and not to be introspected or modified (even as a copy)
var Warehouse_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.Warehouse",
	HandlerType: (*WarehouseServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetHealth",
			Handler:    _Warehouse_GetHealth_Handler,
		},
		{
			MethodName: "GetWHUploads",
			Handler:    _Warehouse_GetWHUploads_Handler,
		},
		{
			MethodName: "GetWHUpload",
			Handler:    _Warehouse_GetWHUpload_Handler,
		},
		{
			MethodName: "TriggerWHUpload",
			Handler:    _Warehouse_TriggerWHUpload_Handler,
		},
		{
			MethodName: "TriggerWHUploads",
			Handler:    _Warehouse_TriggerWHUploads_Handler,
		},
		{
			MethodName: "Validate",
			Handler:    _Warehouse_Validate_Handler,
		},
		{
			MethodName: "RetryWHUploads",
			Handler:    _Warehouse_RetryWHUploads_Handler,
		},
		{
			MethodName: "ValidateObjectStorageDestination",
			Handler:    _Warehouse_ValidateObjectStorageDestination_Handler,
		},
		{
			MethodName: "CountWHUploadsToRetry",
			Handler:    _Warehouse_CountWHUploadsToRetry_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/warehouse/warehouse.proto",
}
