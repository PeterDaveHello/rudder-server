// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// WarehouseClient is the client API for Warehouse service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type WarehouseClient interface {
	GetWHUploads(ctx context.Context, in *GetWHUploadsRequest, opts ...grpc.CallOption) (*GetWHUploadsResponse, error)
	GetWHUpload(ctx context.Context, in *GetWHUploadRequest, opts ...grpc.CallOption) (*GetWHUploadResponse, error)
	GetWHTables(ctx context.Context, in *GetWHTablesRequest, opts ...grpc.CallOption) (*GetWHTablesResponse, error)
}

type warehouseClient struct {
	cc grpc.ClientConnInterface
}

func NewWarehouseClient(cc grpc.ClientConnInterface) WarehouseClient {
	return &warehouseClient{cc}
}

func (c *warehouseClient) GetWHUploads(ctx context.Context, in *GetWHUploadsRequest, opts ...grpc.CallOption) (*GetWHUploadsResponse, error) {
	out := new(GetWHUploadsResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/GetWHUploads", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *warehouseClient) GetWHUpload(ctx context.Context, in *GetWHUploadRequest, opts ...grpc.CallOption) (*GetWHUploadResponse, error) {
	out := new(GetWHUploadResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/GetWHUpload", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *warehouseClient) GetWHTables(ctx context.Context, in *GetWHTablesRequest, opts ...grpc.CallOption) (*GetWHTablesResponse, error) {
	out := new(GetWHTablesResponse)
	err := c.cc.Invoke(ctx, "/proto.Warehouse/GetWHTables", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WarehouseServer is the server API for Warehouse service.
// All implementations must embed UnimplementedWarehouseServer
// for forward compatibility
type WarehouseServer interface {
	GetWHUploads(context.Context, *GetWHUploadsRequest) (*GetWHUploadsResponse, error)
	GetWHUpload(context.Context, *GetWHUploadRequest) (*GetWHUploadResponse, error)
	GetWHTables(context.Context, *GetWHTablesRequest) (*GetWHTablesResponse, error)
	mustEmbedUnimplementedWarehouseServer()
}

// UnimplementedWarehouseServer must be embedded to have forward compatible implementations.
type UnimplementedWarehouseServer struct {
}

func (UnimplementedWarehouseServer) GetWHUploads(context.Context, *GetWHUploadsRequest) (*GetWHUploadsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWHUploads not implemented")
}
func (UnimplementedWarehouseServer) GetWHUpload(context.Context, *GetWHUploadRequest) (*GetWHUploadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWHUpload not implemented")
}
func (UnimplementedWarehouseServer) GetWHTables(context.Context, *GetWHTablesRequest) (*GetWHTablesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWHTables not implemented")
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

func _Warehouse_GetWHUploads_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetWHUploadsRequest)
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
		return srv.(WarehouseServer).GetWHUploads(ctx, req.(*GetWHUploadsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Warehouse_GetWHUpload_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetWHUploadRequest)
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
		return srv.(WarehouseServer).GetWHUpload(ctx, req.(*GetWHUploadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Warehouse_GetWHTables_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetWHTablesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WarehouseServer).GetWHTables(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.Warehouse/GetWHTables",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WarehouseServer).GetWHTables(ctx, req.(*GetWHTablesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Warehouse_ServiceDesc is the grpc.ServiceDesc for Warehouse service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Warehouse_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.Warehouse",
	HandlerType: (*WarehouseServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetWHUploads",
			Handler:    _Warehouse_GetWHUploads_Handler,
		},
		{
			MethodName: "GetWHUpload",
			Handler:    _Warehouse_GetWHUpload_Handler,
		},
		{
			MethodName: "GetWHTables",
			Handler:    _Warehouse_GetWHTables_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/warehouse/warehouse.proto",
}
