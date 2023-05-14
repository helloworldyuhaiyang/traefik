package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/traefik/traefik/v2/pkg/log"
	"io"
	"net/http"
	"strings"
	"sync"
)

var (
	grpcOnce   sync.Once
	grpcClient *GrpcClient
)

type GrpcProxy struct {
	ctx    context.Context
	next   http.Handler
	logger log.Logger
}

func NewGrpcProxy(ctx context.Context, next http.Handler) *GrpcProxy {
	grpcOnce.Do(func() {
		grpcClient = NewGrpcClient()
	})

	return &GrpcProxy{ctx: ctx, logger: log.FromContext(ctx), next: next}
}

func (g *GrpcProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// not grpc scheme
	if req.URL.Scheme != "grpc" {
		g.next.ServeHTTP(w, req)
		return
	}

	g.logger.Debugf("serve grpc service, req.URL: %v", req.URL)

	invokerInfo, err := g.parseReq(req)
	if err != nil {
		g.logger.Errorf("serve grpc service, req.URL: %v", req.URL, err)
		return
	}
	// grpc invoke
	rsp, err := grpcClient.Invoke(context.Background(), invokerInfo.Addr, invokerInfo.ServerName, invokerInfo.MethodName, invokerInfo.ParamsJson)
	if err != nil {
		g.logger.Debugf("Error while grpc Invoke,err: %v", err)
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	marshal, err := json.Marshal(rsp)
	if err != nil {
		g.logger.Debugf("Error while marshal rsp, %v", rsp)
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	w.WriteHeader(200)
	_, err = w.Write(marshal)
	if err != nil {
		g.logger.Debugf("Error while writing, err: %v", err)
		return
	}
}

type GrpcInvokeInfo struct {
	Addr, ServerName, MethodName, ParamsJson string
}

func (g *GrpcProxy) parseReq(req *http.Request) (info *GrpcInvokeInfo, err error) {
	//req.URL
	paths := strings.Split(req.RequestURI, "/")
	num := len(paths)
	if num < 2 {
		err = errors.New("requestURI can not parse ServerName, methodName")
		return
	}

	// body
	contentType := req.Header.Get("Content-Type")
	if contentType != "application/json" {
		err = errors.New("Content-Type is not eq application/json")
		return
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return
	}

	return &GrpcInvokeInfo{
		Addr:       req.URL.Host,
		ServerName: paths[num-2],
		MethodName: paths[num-1],
		ParamsJson: string(body),
	}, nil
}
