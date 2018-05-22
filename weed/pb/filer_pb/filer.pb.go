// Code generated by protoc-gen-go.
// source: filer.proto
// DO NOT EDIT!

/*
Package filer_pb is a generated protocol buffer package.

It is generated from these files:
	filer.proto

It has these top-level messages:
	LookupDirectoryEntryRequest
	LookupDirectoryEntryResponse
	ListEntriesRequest
	ListEntriesResponse
	Entry
	FileChunk
	FuseAttributes
	GetEntryAttributesRequest
	GetEntryAttributesResponse
	GetFileContentRequest
	GetFileContentResponse
	CreateEntryRequest
	CreateEntryResponse
	DeleteEntryRequest
	DeleteEntryResponse
	AssignVolumeRequest
	AssignVolumeResponse
	UpdateEntryRequest
	UpdateEntryResponse
*/
package filer_pb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type LookupDirectoryEntryRequest struct {
	Directory string `protobuf:"bytes,1,opt,name=directory" json:"directory,omitempty"`
	Name      string `protobuf:"bytes,2,opt,name=name" json:"name,omitempty"`
}

func (m *LookupDirectoryEntryRequest) Reset()                    { *m = LookupDirectoryEntryRequest{} }
func (m *LookupDirectoryEntryRequest) String() string            { return proto.CompactTextString(m) }
func (*LookupDirectoryEntryRequest) ProtoMessage()               {}
func (*LookupDirectoryEntryRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *LookupDirectoryEntryRequest) GetDirectory() string {
	if m != nil {
		return m.Directory
	}
	return ""
}

func (m *LookupDirectoryEntryRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type LookupDirectoryEntryResponse struct {
	Entry *Entry `protobuf:"bytes,1,opt,name=entry" json:"entry,omitempty"`
}

func (m *LookupDirectoryEntryResponse) Reset()                    { *m = LookupDirectoryEntryResponse{} }
func (m *LookupDirectoryEntryResponse) String() string            { return proto.CompactTextString(m) }
func (*LookupDirectoryEntryResponse) ProtoMessage()               {}
func (*LookupDirectoryEntryResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *LookupDirectoryEntryResponse) GetEntry() *Entry {
	if m != nil {
		return m.Entry
	}
	return nil
}

type ListEntriesRequest struct {
	Directory string `protobuf:"bytes,1,opt,name=directory" json:"directory,omitempty"`
}

func (m *ListEntriesRequest) Reset()                    { *m = ListEntriesRequest{} }
func (m *ListEntriesRequest) String() string            { return proto.CompactTextString(m) }
func (*ListEntriesRequest) ProtoMessage()               {}
func (*ListEntriesRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *ListEntriesRequest) GetDirectory() string {
	if m != nil {
		return m.Directory
	}
	return ""
}

type ListEntriesResponse struct {
	Entries []*Entry `protobuf:"bytes,1,rep,name=entries" json:"entries,omitempty"`
}

func (m *ListEntriesResponse) Reset()                    { *m = ListEntriesResponse{} }
func (m *ListEntriesResponse) String() string            { return proto.CompactTextString(m) }
func (*ListEntriesResponse) ProtoMessage()               {}
func (*ListEntriesResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func (m *ListEntriesResponse) GetEntries() []*Entry {
	if m != nil {
		return m.Entries
	}
	return nil
}

type Entry struct {
	Name        string          `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	IsDirectory bool            `protobuf:"varint,2,opt,name=is_directory,json=isDirectory" json:"is_directory,omitempty"`
	Chunks      []*FileChunk    `protobuf:"bytes,3,rep,name=chunks" json:"chunks,omitempty"`
	Attributes  *FuseAttributes `protobuf:"bytes,4,opt,name=attributes" json:"attributes,omitempty"`
}

func (m *Entry) Reset()                    { *m = Entry{} }
func (m *Entry) String() string            { return proto.CompactTextString(m) }
func (*Entry) ProtoMessage()               {}
func (*Entry) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *Entry) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Entry) GetIsDirectory() bool {
	if m != nil {
		return m.IsDirectory
	}
	return false
}

func (m *Entry) GetChunks() []*FileChunk {
	if m != nil {
		return m.Chunks
	}
	return nil
}

func (m *Entry) GetAttributes() *FuseAttributes {
	if m != nil {
		return m.Attributes
	}
	return nil
}

type FileChunk struct {
	FileId string `protobuf:"bytes,1,opt,name=file_id,json=fileId" json:"file_id,omitempty"`
	Offset int64  `protobuf:"varint,2,opt,name=offset" json:"offset,omitempty"`
	Size   uint64 `protobuf:"varint,3,opt,name=size" json:"size,omitempty"`
	Mtime  int64  `protobuf:"varint,4,opt,name=mtime" json:"mtime,omitempty"`
}

func (m *FileChunk) Reset()                    { *m = FileChunk{} }
func (m *FileChunk) String() string            { return proto.CompactTextString(m) }
func (*FileChunk) ProtoMessage()               {}
func (*FileChunk) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *FileChunk) GetFileId() string {
	if m != nil {
		return m.FileId
	}
	return ""
}

func (m *FileChunk) GetOffset() int64 {
	if m != nil {
		return m.Offset
	}
	return 0
}

func (m *FileChunk) GetSize() uint64 {
	if m != nil {
		return m.Size
	}
	return 0
}

func (m *FileChunk) GetMtime() int64 {
	if m != nil {
		return m.Mtime
	}
	return 0
}

type FuseAttributes struct {
	FileSize uint64 `protobuf:"varint,1,opt,name=file_size,json=fileSize" json:"file_size,omitempty"`
	Mtime    int64  `protobuf:"varint,2,opt,name=mtime" json:"mtime,omitempty"`
	FileMode uint32 `protobuf:"varint,3,opt,name=file_mode,json=fileMode" json:"file_mode,omitempty"`
	Uid      uint32 `protobuf:"varint,4,opt,name=uid" json:"uid,omitempty"`
	Gid      uint32 `protobuf:"varint,5,opt,name=gid" json:"gid,omitempty"`
}

func (m *FuseAttributes) Reset()                    { *m = FuseAttributes{} }
func (m *FuseAttributes) String() string            { return proto.CompactTextString(m) }
func (*FuseAttributes) ProtoMessage()               {}
func (*FuseAttributes) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

func (m *FuseAttributes) GetFileSize() uint64 {
	if m != nil {
		return m.FileSize
	}
	return 0
}

func (m *FuseAttributes) GetMtime() int64 {
	if m != nil {
		return m.Mtime
	}
	return 0
}

func (m *FuseAttributes) GetFileMode() uint32 {
	if m != nil {
		return m.FileMode
	}
	return 0
}

func (m *FuseAttributes) GetUid() uint32 {
	if m != nil {
		return m.Uid
	}
	return 0
}

func (m *FuseAttributes) GetGid() uint32 {
	if m != nil {
		return m.Gid
	}
	return 0
}

type GetEntryAttributesRequest struct {
	Name      string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	ParentDir string `protobuf:"bytes,2,opt,name=parent_dir,json=parentDir" json:"parent_dir,omitempty"`
	FileId    string `protobuf:"bytes,3,opt,name=file_id,json=fileId" json:"file_id,omitempty"`
}

func (m *GetEntryAttributesRequest) Reset()                    { *m = GetEntryAttributesRequest{} }
func (m *GetEntryAttributesRequest) String() string            { return proto.CompactTextString(m) }
func (*GetEntryAttributesRequest) ProtoMessage()               {}
func (*GetEntryAttributesRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

func (m *GetEntryAttributesRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *GetEntryAttributesRequest) GetParentDir() string {
	if m != nil {
		return m.ParentDir
	}
	return ""
}

func (m *GetEntryAttributesRequest) GetFileId() string {
	if m != nil {
		return m.FileId
	}
	return ""
}

type GetEntryAttributesResponse struct {
	Attributes *FuseAttributes `protobuf:"bytes,1,opt,name=attributes" json:"attributes,omitempty"`
	Chunks     []*FileChunk    `protobuf:"bytes,2,rep,name=chunks" json:"chunks,omitempty"`
}

func (m *GetEntryAttributesResponse) Reset()                    { *m = GetEntryAttributesResponse{} }
func (m *GetEntryAttributesResponse) String() string            { return proto.CompactTextString(m) }
func (*GetEntryAttributesResponse) ProtoMessage()               {}
func (*GetEntryAttributesResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{8} }

func (m *GetEntryAttributesResponse) GetAttributes() *FuseAttributes {
	if m != nil {
		return m.Attributes
	}
	return nil
}

func (m *GetEntryAttributesResponse) GetChunks() []*FileChunk {
	if m != nil {
		return m.Chunks
	}
	return nil
}

type GetFileContentRequest struct {
	FileId string `protobuf:"bytes,1,opt,name=file_id,json=fileId" json:"file_id,omitempty"`
}

func (m *GetFileContentRequest) Reset()                    { *m = GetFileContentRequest{} }
func (m *GetFileContentRequest) String() string            { return proto.CompactTextString(m) }
func (*GetFileContentRequest) ProtoMessage()               {}
func (*GetFileContentRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{9} }

func (m *GetFileContentRequest) GetFileId() string {
	if m != nil {
		return m.FileId
	}
	return ""
}

type GetFileContentResponse struct {
	Content []byte `protobuf:"bytes,1,opt,name=content,proto3" json:"content,omitempty"`
}

func (m *GetFileContentResponse) Reset()                    { *m = GetFileContentResponse{} }
func (m *GetFileContentResponse) String() string            { return proto.CompactTextString(m) }
func (*GetFileContentResponse) ProtoMessage()               {}
func (*GetFileContentResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{10} }

func (m *GetFileContentResponse) GetContent() []byte {
	if m != nil {
		return m.Content
	}
	return nil
}

type CreateEntryRequest struct {
	Directory string `protobuf:"bytes,1,opt,name=directory" json:"directory,omitempty"`
	Entry     *Entry `protobuf:"bytes,2,opt,name=entry" json:"entry,omitempty"`
}

func (m *CreateEntryRequest) Reset()                    { *m = CreateEntryRequest{} }
func (m *CreateEntryRequest) String() string            { return proto.CompactTextString(m) }
func (*CreateEntryRequest) ProtoMessage()               {}
func (*CreateEntryRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{11} }

func (m *CreateEntryRequest) GetDirectory() string {
	if m != nil {
		return m.Directory
	}
	return ""
}

func (m *CreateEntryRequest) GetEntry() *Entry {
	if m != nil {
		return m.Entry
	}
	return nil
}

type CreateEntryResponse struct {
}

func (m *CreateEntryResponse) Reset()                    { *m = CreateEntryResponse{} }
func (m *CreateEntryResponse) String() string            { return proto.CompactTextString(m) }
func (*CreateEntryResponse) ProtoMessage()               {}
func (*CreateEntryResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{12} }

type DeleteEntryRequest struct {
	Directory   string `protobuf:"bytes,1,opt,name=directory" json:"directory,omitempty"`
	Name        string `protobuf:"bytes,2,opt,name=name" json:"name,omitempty"`
	IsDirectory bool   `protobuf:"varint,3,opt,name=is_directory,json=isDirectory" json:"is_directory,omitempty"`
}

func (m *DeleteEntryRequest) Reset()                    { *m = DeleteEntryRequest{} }
func (m *DeleteEntryRequest) String() string            { return proto.CompactTextString(m) }
func (*DeleteEntryRequest) ProtoMessage()               {}
func (*DeleteEntryRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{13} }

func (m *DeleteEntryRequest) GetDirectory() string {
	if m != nil {
		return m.Directory
	}
	return ""
}

func (m *DeleteEntryRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *DeleteEntryRequest) GetIsDirectory() bool {
	if m != nil {
		return m.IsDirectory
	}
	return false
}

type DeleteEntryResponse struct {
}

func (m *DeleteEntryResponse) Reset()                    { *m = DeleteEntryResponse{} }
func (m *DeleteEntryResponse) String() string            { return proto.CompactTextString(m) }
func (*DeleteEntryResponse) ProtoMessage()               {}
func (*DeleteEntryResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{14} }

type AssignVolumeRequest struct {
	Count       int32  `protobuf:"varint,1,opt,name=count" json:"count,omitempty"`
	Collection  string `protobuf:"bytes,2,opt,name=collection" json:"collection,omitempty"`
	Replication string `protobuf:"bytes,3,opt,name=replication" json:"replication,omitempty"`
}

func (m *AssignVolumeRequest) Reset()                    { *m = AssignVolumeRequest{} }
func (m *AssignVolumeRequest) String() string            { return proto.CompactTextString(m) }
func (*AssignVolumeRequest) ProtoMessage()               {}
func (*AssignVolumeRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{15} }

func (m *AssignVolumeRequest) GetCount() int32 {
	if m != nil {
		return m.Count
	}
	return 0
}

func (m *AssignVolumeRequest) GetCollection() string {
	if m != nil {
		return m.Collection
	}
	return ""
}

func (m *AssignVolumeRequest) GetReplication() string {
	if m != nil {
		return m.Replication
	}
	return ""
}

type AssignVolumeResponse struct {
	FileId    string `protobuf:"bytes,1,opt,name=file_id,json=fileId" json:"file_id,omitempty"`
	Url       string `protobuf:"bytes,2,opt,name=url" json:"url,omitempty"`
	PublicUrl string `protobuf:"bytes,3,opt,name=public_url,json=publicUrl" json:"public_url,omitempty"`
	Count     int32  `protobuf:"varint,4,opt,name=count" json:"count,omitempty"`
}

func (m *AssignVolumeResponse) Reset()                    { *m = AssignVolumeResponse{} }
func (m *AssignVolumeResponse) String() string            { return proto.CompactTextString(m) }
func (*AssignVolumeResponse) ProtoMessage()               {}
func (*AssignVolumeResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{16} }

func (m *AssignVolumeResponse) GetFileId() string {
	if m != nil {
		return m.FileId
	}
	return ""
}

func (m *AssignVolumeResponse) GetUrl() string {
	if m != nil {
		return m.Url
	}
	return ""
}

func (m *AssignVolumeResponse) GetPublicUrl() string {
	if m != nil {
		return m.PublicUrl
	}
	return ""
}

func (m *AssignVolumeResponse) GetCount() int32 {
	if m != nil {
		return m.Count
	}
	return 0
}

type UpdateEntryRequest struct {
	Directory string `protobuf:"bytes,1,opt,name=directory" json:"directory,omitempty"`
	Entry     *Entry `protobuf:"bytes,2,opt,name=entry" json:"entry,omitempty"`
}

func (m *UpdateEntryRequest) Reset()                    { *m = UpdateEntryRequest{} }
func (m *UpdateEntryRequest) String() string            { return proto.CompactTextString(m) }
func (*UpdateEntryRequest) ProtoMessage()               {}
func (*UpdateEntryRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{17} }

func (m *UpdateEntryRequest) GetDirectory() string {
	if m != nil {
		return m.Directory
	}
	return ""
}

func (m *UpdateEntryRequest) GetEntry() *Entry {
	if m != nil {
		return m.Entry
	}
	return nil
}

type UpdateEntryResponse struct {
}

func (m *UpdateEntryResponse) Reset()                    { *m = UpdateEntryResponse{} }
func (m *UpdateEntryResponse) String() string            { return proto.CompactTextString(m) }
func (*UpdateEntryResponse) ProtoMessage()               {}
func (*UpdateEntryResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{18} }

func init() {
	proto.RegisterType((*LookupDirectoryEntryRequest)(nil), "filer_pb.LookupDirectoryEntryRequest")
	proto.RegisterType((*LookupDirectoryEntryResponse)(nil), "filer_pb.LookupDirectoryEntryResponse")
	proto.RegisterType((*ListEntriesRequest)(nil), "filer_pb.ListEntriesRequest")
	proto.RegisterType((*ListEntriesResponse)(nil), "filer_pb.ListEntriesResponse")
	proto.RegisterType((*Entry)(nil), "filer_pb.Entry")
	proto.RegisterType((*FileChunk)(nil), "filer_pb.FileChunk")
	proto.RegisterType((*FuseAttributes)(nil), "filer_pb.FuseAttributes")
	proto.RegisterType((*GetEntryAttributesRequest)(nil), "filer_pb.GetEntryAttributesRequest")
	proto.RegisterType((*GetEntryAttributesResponse)(nil), "filer_pb.GetEntryAttributesResponse")
	proto.RegisterType((*GetFileContentRequest)(nil), "filer_pb.GetFileContentRequest")
	proto.RegisterType((*GetFileContentResponse)(nil), "filer_pb.GetFileContentResponse")
	proto.RegisterType((*CreateEntryRequest)(nil), "filer_pb.CreateEntryRequest")
	proto.RegisterType((*CreateEntryResponse)(nil), "filer_pb.CreateEntryResponse")
	proto.RegisterType((*DeleteEntryRequest)(nil), "filer_pb.DeleteEntryRequest")
	proto.RegisterType((*DeleteEntryResponse)(nil), "filer_pb.DeleteEntryResponse")
	proto.RegisterType((*AssignVolumeRequest)(nil), "filer_pb.AssignVolumeRequest")
	proto.RegisterType((*AssignVolumeResponse)(nil), "filer_pb.AssignVolumeResponse")
	proto.RegisterType((*UpdateEntryRequest)(nil), "filer_pb.UpdateEntryRequest")
	proto.RegisterType((*UpdateEntryResponse)(nil), "filer_pb.UpdateEntryResponse")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for SeaweedFiler service

type SeaweedFilerClient interface {
	LookupDirectoryEntry(ctx context.Context, in *LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*LookupDirectoryEntryResponse, error)
	ListEntries(ctx context.Context, in *ListEntriesRequest, opts ...grpc.CallOption) (*ListEntriesResponse, error)
	GetEntryAttributes(ctx context.Context, in *GetEntryAttributesRequest, opts ...grpc.CallOption) (*GetEntryAttributesResponse, error)
	GetFileContent(ctx context.Context, in *GetFileContentRequest, opts ...grpc.CallOption) (*GetFileContentResponse, error)
	CreateEntry(ctx context.Context, in *CreateEntryRequest, opts ...grpc.CallOption) (*CreateEntryResponse, error)
	UpdateEntry(ctx context.Context, in *UpdateEntryRequest, opts ...grpc.CallOption) (*UpdateEntryResponse, error)
	DeleteEntry(ctx context.Context, in *DeleteEntryRequest, opts ...grpc.CallOption) (*DeleteEntryResponse, error)
	AssignVolume(ctx context.Context, in *AssignVolumeRequest, opts ...grpc.CallOption) (*AssignVolumeResponse, error)
}

type seaweedFilerClient struct {
	cc *grpc.ClientConn
}

func NewSeaweedFilerClient(cc *grpc.ClientConn) SeaweedFilerClient {
	return &seaweedFilerClient{cc}
}

func (c *seaweedFilerClient) LookupDirectoryEntry(ctx context.Context, in *LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*LookupDirectoryEntryResponse, error) {
	out := new(LookupDirectoryEntryResponse)
	err := grpc.Invoke(ctx, "/filer_pb.SeaweedFiler/LookupDirectoryEntry", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *seaweedFilerClient) ListEntries(ctx context.Context, in *ListEntriesRequest, opts ...grpc.CallOption) (*ListEntriesResponse, error) {
	out := new(ListEntriesResponse)
	err := grpc.Invoke(ctx, "/filer_pb.SeaweedFiler/ListEntries", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *seaweedFilerClient) GetEntryAttributes(ctx context.Context, in *GetEntryAttributesRequest, opts ...grpc.CallOption) (*GetEntryAttributesResponse, error) {
	out := new(GetEntryAttributesResponse)
	err := grpc.Invoke(ctx, "/filer_pb.SeaweedFiler/GetEntryAttributes", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *seaweedFilerClient) GetFileContent(ctx context.Context, in *GetFileContentRequest, opts ...grpc.CallOption) (*GetFileContentResponse, error) {
	out := new(GetFileContentResponse)
	err := grpc.Invoke(ctx, "/filer_pb.SeaweedFiler/GetFileContent", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *seaweedFilerClient) CreateEntry(ctx context.Context, in *CreateEntryRequest, opts ...grpc.CallOption) (*CreateEntryResponse, error) {
	out := new(CreateEntryResponse)
	err := grpc.Invoke(ctx, "/filer_pb.SeaweedFiler/CreateEntry", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *seaweedFilerClient) UpdateEntry(ctx context.Context, in *UpdateEntryRequest, opts ...grpc.CallOption) (*UpdateEntryResponse, error) {
	out := new(UpdateEntryResponse)
	err := grpc.Invoke(ctx, "/filer_pb.SeaweedFiler/UpdateEntry", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *seaweedFilerClient) DeleteEntry(ctx context.Context, in *DeleteEntryRequest, opts ...grpc.CallOption) (*DeleteEntryResponse, error) {
	out := new(DeleteEntryResponse)
	err := grpc.Invoke(ctx, "/filer_pb.SeaweedFiler/DeleteEntry", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *seaweedFilerClient) AssignVolume(ctx context.Context, in *AssignVolumeRequest, opts ...grpc.CallOption) (*AssignVolumeResponse, error) {
	out := new(AssignVolumeResponse)
	err := grpc.Invoke(ctx, "/filer_pb.SeaweedFiler/AssignVolume", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for SeaweedFiler service

type SeaweedFilerServer interface {
	LookupDirectoryEntry(context.Context, *LookupDirectoryEntryRequest) (*LookupDirectoryEntryResponse, error)
	ListEntries(context.Context, *ListEntriesRequest) (*ListEntriesResponse, error)
	GetEntryAttributes(context.Context, *GetEntryAttributesRequest) (*GetEntryAttributesResponse, error)
	GetFileContent(context.Context, *GetFileContentRequest) (*GetFileContentResponse, error)
	CreateEntry(context.Context, *CreateEntryRequest) (*CreateEntryResponse, error)
	UpdateEntry(context.Context, *UpdateEntryRequest) (*UpdateEntryResponse, error)
	DeleteEntry(context.Context, *DeleteEntryRequest) (*DeleteEntryResponse, error)
	AssignVolume(context.Context, *AssignVolumeRequest) (*AssignVolumeResponse, error)
}

func RegisterSeaweedFilerServer(s *grpc.Server, srv SeaweedFilerServer) {
	s.RegisterService(&_SeaweedFiler_serviceDesc, srv)
}

func _SeaweedFiler_LookupDirectoryEntry_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LookupDirectoryEntryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeaweedFilerServer).LookupDirectoryEntry(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/filer_pb.SeaweedFiler/LookupDirectoryEntry",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeaweedFilerServer).LookupDirectoryEntry(ctx, req.(*LookupDirectoryEntryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SeaweedFiler_ListEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListEntriesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeaweedFilerServer).ListEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/filer_pb.SeaweedFiler/ListEntries",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeaweedFilerServer).ListEntries(ctx, req.(*ListEntriesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SeaweedFiler_GetEntryAttributes_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetEntryAttributesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeaweedFilerServer).GetEntryAttributes(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/filer_pb.SeaweedFiler/GetEntryAttributes",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeaweedFilerServer).GetEntryAttributes(ctx, req.(*GetEntryAttributesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SeaweedFiler_GetFileContent_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetFileContentRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeaweedFilerServer).GetFileContent(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/filer_pb.SeaweedFiler/GetFileContent",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeaweedFilerServer).GetFileContent(ctx, req.(*GetFileContentRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SeaweedFiler_CreateEntry_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateEntryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeaweedFilerServer).CreateEntry(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/filer_pb.SeaweedFiler/CreateEntry",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeaweedFilerServer).CreateEntry(ctx, req.(*CreateEntryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SeaweedFiler_UpdateEntry_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateEntryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeaweedFilerServer).UpdateEntry(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/filer_pb.SeaweedFiler/UpdateEntry",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeaweedFilerServer).UpdateEntry(ctx, req.(*UpdateEntryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SeaweedFiler_DeleteEntry_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteEntryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeaweedFilerServer).DeleteEntry(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/filer_pb.SeaweedFiler/DeleteEntry",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeaweedFilerServer).DeleteEntry(ctx, req.(*DeleteEntryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SeaweedFiler_AssignVolume_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AssignVolumeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeaweedFilerServer).AssignVolume(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/filer_pb.SeaweedFiler/AssignVolume",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeaweedFilerServer).AssignVolume(ctx, req.(*AssignVolumeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _SeaweedFiler_serviceDesc = grpc.ServiceDesc{
	ServiceName: "filer_pb.SeaweedFiler",
	HandlerType: (*SeaweedFilerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "LookupDirectoryEntry",
			Handler:    _SeaweedFiler_LookupDirectoryEntry_Handler,
		},
		{
			MethodName: "ListEntries",
			Handler:    _SeaweedFiler_ListEntries_Handler,
		},
		{
			MethodName: "GetEntryAttributes",
			Handler:    _SeaweedFiler_GetEntryAttributes_Handler,
		},
		{
			MethodName: "GetFileContent",
			Handler:    _SeaweedFiler_GetFileContent_Handler,
		},
		{
			MethodName: "CreateEntry",
			Handler:    _SeaweedFiler_CreateEntry_Handler,
		},
		{
			MethodName: "UpdateEntry",
			Handler:    _SeaweedFiler_UpdateEntry_Handler,
		},
		{
			MethodName: "DeleteEntry",
			Handler:    _SeaweedFiler_DeleteEntry_Handler,
		},
		{
			MethodName: "AssignVolume",
			Handler:    _SeaweedFiler_AssignVolume_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "filer.proto",
}

func init() { proto.RegisterFile("filer.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 763 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xb4, 0x56, 0xdd, 0x6e, 0xd3, 0x4a,
	0x10, 0xae, 0xe3, 0x24, 0x6d, 0x26, 0x69, 0xcf, 0xd1, 0x26, 0xed, 0xf1, 0x49, 0x7f, 0x08, 0x86,
	0xa2, 0x22, 0xa4, 0x0a, 0x85, 0x1b, 0x2e, 0xa9, 0xda, 0x52, 0x21, 0x15, 0x55, 0x72, 0x55, 0x24,
	0xae, 0xa2, 0xc4, 0x9e, 0x84, 0x55, 0x1d, 0x3b, 0x78, 0xd7, 0xa0, 0x72, 0x0b, 0xaf, 0xc2, 0x4b,
	0xf0, 0x74, 0x68, 0x7f, 0xe2, 0xac, 0xb1, 0xd3, 0x9f, 0x0b, 0xee, 0x76, 0x67, 0x76, 0xbe, 0xf9,
	0x76, 0x66, 0xbe, 0xb5, 0xa1, 0x39, 0xa6, 0x21, 0x26, 0x87, 0xb3, 0x24, 0xe6, 0x31, 0x59, 0x93,
	0x9b, 0xc1, 0x6c, 0xe4, 0x5e, 0xc0, 0xf6, 0x79, 0x1c, 0x5f, 0xa7, 0xb3, 0x13, 0x9a, 0xa0, 0xcf,
	0xe3, 0xe4, 0xe6, 0x34, 0xe2, 0xc9, 0x8d, 0x87, 0x9f, 0x53, 0x64, 0x9c, 0xec, 0x40, 0x23, 0x98,
	0x3b, 0x1c, 0xab, 0x67, 0x1d, 0x34, 0xbc, 0x85, 0x81, 0x10, 0xa8, 0x46, 0xc3, 0x29, 0x3a, 0x15,
	0xe9, 0x90, 0x6b, 0xf7, 0x14, 0x76, 0xca, 0x01, 0xd9, 0x2c, 0x8e, 0x18, 0x92, 0x7d, 0xa8, 0xa1,
	0x30, 0x48, 0xb4, 0x66, 0xff, 0x9f, 0xc3, 0x39, 0x95, 0x43, 0x75, 0x4e, 0x79, 0xdd, 0x3e, 0x90,
	0x73, 0xca, 0xb8, 0xb0, 0x51, 0x64, 0xf7, 0xa2, 0xe3, 0xbe, 0x81, 0x76, 0x2e, 0x46, 0x67, 0x7c,
	0x0e, 0xab, 0xa8, 0x4c, 0x8e, 0xd5, 0xb3, 0xcb, 0x72, 0xce, 0xfd, 0xee, 0x4f, 0x0b, 0x6a, 0xd2,
	0x94, 0x5d, 0xcd, 0x5a, 0x5c, 0x8d, 0x3c, 0x86, 0x16, 0x65, 0x83, 0x05, 0x01, 0x71, 0xed, 0x35,
	0xaf, 0x49, 0x59, 0x76, 0x55, 0xf2, 0x02, 0xea, 0xfe, 0xa7, 0x34, 0xba, 0x66, 0x8e, 0x2d, 0x53,
	0xb5, 0x17, 0xa9, 0xde, 0xd2, 0x10, 0x8f, 0x85, 0xcf, 0xd3, 0x47, 0xc8, 0x6b, 0x80, 0x21, 0xe7,
	0x09, 0x1d, 0xa5, 0x1c, 0x99, 0x53, 0x95, 0xf5, 0x70, 0x8c, 0x80, 0x94, 0xe1, 0x51, 0xe6, 0xf7,
	0x8c, 0xb3, 0xee, 0x18, 0x1a, 0x19, 0x1c, 0xf9, 0x0f, 0x56, 0x45, 0xcc, 0x80, 0x06, 0x9a, 0x6d,
	0x5d, 0x6c, 0xdf, 0x05, 0x64, 0x0b, 0xea, 0xf1, 0x78, 0xcc, 0x90, 0x4b, 0xa6, 0xb6, 0xa7, 0x77,
	0xe2, 0x6e, 0x8c, 0x7e, 0x43, 0xc7, 0xee, 0x59, 0x07, 0x55, 0x4f, 0xae, 0x49, 0x07, 0x6a, 0x53,
	0x4e, 0xa7, 0x28, 0x69, 0xd8, 0x9e, 0xda, 0xb8, 0x3f, 0x2c, 0xd8, 0xc8, 0xd3, 0x20, 0xdb, 0xd0,
	0x90, 0xd9, 0x24, 0x82, 0x25, 0x11, 0xe4, 0x34, 0x5d, 0xe6, 0x50, 0x2a, 0x06, 0x4a, 0x16, 0x32,
	0x8d, 0x03, 0x95, 0x74, 0x5d, 0x85, 0xbc, 0x8f, 0x03, 0x24, 0xff, 0x82, 0x9d, 0xd2, 0x40, 0xa6,
	0x5d, 0xf7, 0xc4, 0x52, 0x58, 0x26, 0x34, 0x70, 0x6a, 0xca, 0x32, 0xa1, 0x81, 0x3b, 0x81, 0xff,
	0xcf, 0x50, 0xf6, 0xf5, 0xc6, 0x28, 0x88, 0x9e, 0x89, 0xb2, 0x4e, 0xed, 0x02, 0xcc, 0x86, 0x09,
	0x46, 0x5c, 0x74, 0x4b, 0x8f, 0x67, 0x43, 0x59, 0x4e, 0x68, 0x62, 0x56, 0xcc, 0x36, 0x2b, 0xe6,
	0x7e, 0xb7, 0xa0, 0x5b, 0x96, 0x49, 0x4f, 0x52, 0xbe, 0x61, 0xd6, 0xfd, 0x1b, 0x66, 0xcc, 0x45,
	0xe5, 0xce, 0xb9, 0x70, 0x5f, 0xc2, 0xe6, 0x19, 0x72, 0x69, 0x8f, 0x23, 0x8e, 0x11, 0x9f, 0x5f,
	0x75, 0x59, 0xa7, 0xdd, 0x3e, 0x6c, 0xfd, 0x19, 0xa1, 0x29, 0x3b, 0xb0, 0xea, 0x2b, 0x93, 0x0c,
	0x69, 0x79, 0xf3, 0xad, 0xfb, 0x11, 0xc8, 0x71, 0x82, 0x43, 0x8e, 0x0f, 0x10, 0x7c, 0x26, 0xde,
	0xca, 0xad, 0xe2, 0xdd, 0x84, 0x76, 0x0e, 0x5a, 0x71, 0x71, 0x29, 0x90, 0x13, 0x0c, 0xf1, 0x41,
	0x19, 0x4b, 0x9e, 0x98, 0x82, 0x0e, 0xed, 0x82, 0x0e, 0x05, 0x83, 0x5c, 0x2a, 0xcd, 0x60, 0x0a,
	0xed, 0x23, 0xc6, 0xe8, 0x24, 0xfa, 0x10, 0x87, 0xe9, 0x14, 0xe7, 0x14, 0x3a, 0x50, 0xf3, 0xe3,
	0x54, 0x97, 0xa8, 0xe6, 0xa9, 0x0d, 0xd9, 0x03, 0xf0, 0xe3, 0x30, 0x44, 0x9f, 0xd3, 0x38, 0xd2,
	0x04, 0x0c, 0x0b, 0xe9, 0x41, 0x33, 0xc1, 0x59, 0x48, 0xfd, 0xa1, 0x3c, 0xa0, 0x26, 0xc9, 0x34,
	0xb9, 0x5f, 0xa0, 0x93, 0x4f, 0xa7, 0x9b, 0xb2, 0x54, 0xb1, 0x42, 0x0c, 0x49, 0xa8, 0x73, 0x89,
	0xa5, 0x9c, 0xe4, 0x74, 0x14, 0x52, 0x7f, 0x20, 0x1c, 0xb6, 0x9e, 0x64, 0x69, 0xb9, 0x4a, 0xc2,
	0x05, 0xf3, 0xaa, 0xc1, 0x5c, 0xb4, 0xf6, 0x6a, 0x16, 0xfc, 0xad, 0xd6, 0xe6, 0xa0, 0xd5, 0x8d,
	0xfa, 0xbf, 0x6a, 0xd0, 0xba, 0xc4, 0xe1, 0x57, 0xc4, 0x40, 0x4c, 0x61, 0x42, 0x26, 0xd0, 0x29,
	0xfb, 0x0c, 0x90, 0xfd, 0x05, 0xee, 0x2d, 0xdf, 0x9d, 0xee, 0xb3, 0xbb, 0x8e, 0xe9, 0x86, 0xae,
	0x90, 0x73, 0x68, 0x1a, 0x8f, 0x3e, 0xd9, 0x31, 0x02, 0x0b, 0xdf, 0x8f, 0xee, 0xee, 0x12, 0x6f,
	0x86, 0x36, 0x04, 0x52, 0xd4, 0x3f, 0x79, 0xb2, 0x08, 0x5b, 0xfa, 0x0e, 0x75, 0x9f, 0xde, 0x7e,
	0x28, 0x4b, 0x71, 0x05, 0x1b, 0x79, 0xad, 0x92, 0x47, 0xb9, 0xc8, 0xa2, 0xee, 0xbb, 0xbd, 0xe5,
	0x07, 0xcc, 0x3a, 0x18, 0x9a, 0x33, 0xeb, 0x50, 0x54, 0xb9, 0x59, 0x87, 0x32, 0xa1, 0x4a, 0x34,
	0xa3, 0xcd, 0x26, 0x5a, 0x71, 0xb0, 0x4c, 0xb4, 0x92, 0xd9, 0x50, 0x68, 0x86, 0x1a, 0x4d, 0xb4,
	0xe2, 0x7b, 0x60, 0xa2, 0x95, 0x49, 0x78, 0x85, 0x5c, 0x40, 0xcb, 0x54, 0x15, 0x31, 0x02, 0x4a,
	0xc4, 0xdd, 0xdd, 0x5b, 0xe6, 0x9e, 0x03, 0x8e, 0xea, 0xf2, 0xa7, 0xe8, 0xd5, 0xef, 0x00, 0x00,
	0x00, 0xff, 0xff, 0xec, 0x38, 0x88, 0xb2, 0x23, 0x09, 0x00, 0x00,
}
