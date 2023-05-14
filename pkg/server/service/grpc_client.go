package service

import (
	"context"
	"encoding/json"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

type GrpcClient struct {
	reflectClient map[string]*ServerInfo
}

func NewGrpcClient() *GrpcClient {
	return &GrpcClient{
		reflectClient: make(map[string]*ServerInfo),
	}
}

func (g *GrpcClient) Invoke(ctx context.Context, addr, serverName, methodName string, paramsJson string) (map[string]interface{}, error) {
	serverInfo, ok := g.reflectClient[serverName]

	if !ok {
		conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		newClient := grpcreflect.NewClient(ctx, rpb.NewServerReflectionClient(conn))
		serverInfo = &ServerInfo{
			conn:      conn,
			refClient: newClient,
		}
		g.reflectClient[serverName] = serverInfo
	}

	serviceDescriptor, err := serverInfo.refClient.ResolveService(serverName)
	if err != nil {
		return nil, err
	}

	methodDescriptor := serviceDescriptor.FindMethodByName(methodName)
	if methodDescriptor == nil {
		return nil, err
	}

	inputType := methodDescriptor.GetInputType()
	msgm := dynamic.NewMessage(inputType)
	err = msgm.UnmarshalJSON([]byte(paramsJson))
	if err != nil {
		return nil, err
	}

	stub := grpcdynamic.NewStub(serverInfo.conn)
	rpcRsp, err := stub.InvokeRpc(ctx, methodDescriptor, msgm)
	if err != nil {
		return nil, err
	}

	dm := rpcRsp.(*dynamic.Message)
	rsp, err := dm.MarshalJSON()
	if err != nil {
		return nil, nil
	}

	t := make(map[string]interface{})
	err = json.Unmarshal(rsp, &t)
	return t, nil
}

type ServerInfo struct {
	conn      *grpc.ClientConn
	refClient *grpcreflect.Client
}
