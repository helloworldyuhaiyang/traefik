package grpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	ctx      context.Context
	addr     string
	conn     *grpc.ClientConn
	grpcStub grpcdynamic.Stub

	updateLock sync.RWMutex
	// serviceDescriptors is a map of service name to service descriptor
	serviceDescriptors *map[string]*desc.ServiceDescriptor
	reflectionTimeout  time.Duration
	invokeTimeout      time.Duration
	// interval of reflecting grpc server
	interval time.Duration
	// flushing is a flag to indicate whether the client is being flushed
	flushing       atomic.Bool
	LastUpdateTime time.Time
}

func NewClient(ctx context.Context, conn *grpc.ClientConn, addr string, reflectionTimeout, invokeTimeout, interval time.Duration) *Client {
	return &Client{
		ctx:                ctx,
		addr:               addr,
		conn:               conn,
		grpcStub:           grpcdynamic.NewStub(conn),
		serviceDescriptors: nil,
		reflectionTimeout:  reflectionTimeout,
		invokeTimeout:      invokeTimeout,
		interval:           interval,
		flushing:           atomic.Bool{},
		LastUpdateTime:     time.Unix(0, 0),
	}
}

func getServiceDescriptor(ctx context.Context, conn *grpc.ClientConn) (serviceDescriptors map[string]*desc.ServiceDescriptor, err error) {
	refClient := grpcreflect.NewClientAuto(ctx, conn)
	defer refClient.Reset()

	services, err := refClient.ListServices()
	if err != nil {
		return nil, err
	}
	serviceDescriptors = make(map[string]*desc.ServiceDescriptor)
	for _, service := range services {
		resolveService, err := refClient.ResolveService(service)
		if err != nil {
			return nil, err
		}
		serviceDescriptors[service] = resolveService
	}

	return serviceDescriptors, nil
}

// flushReflection this function will not block the caller
// flushes the reflection info of the client when the interval time has passed and the client is not being flushed.
func (c *Client) flushReflection() {
	nt := time.Now()
	if c.LastUpdateTime.Add(c.interval).After(nt) {
		return
	}

	// avoid concurrent call
	if c.flushing.CompareAndSwap(false, true) {
		go func() {
			defer c.flushing.Store(false)

			// get dynamic info
			ctx, cancel := context.WithTimeout(context.Background(), c.reflectionTimeout)
			defer cancel()
			serviceDescriptors, err := getServiceDescriptor(ctx, c.conn)
			if err != nil {
				log.Ctx(c.ctx).Err(err).Msg("Get service descriptor failed")
				return
			}
			log.Ctx(c.ctx).Debug().Msgf("Get service descriptor success, addr: %s", c.addr)
			// update serviceDescriptors pointer to new serviceDescriptors
			c.updateLock.Lock()
			defer c.updateLock.Unlock()

			c.updateServiceDescriptorLocked(serviceDescriptors)
			c.LastUpdateTime = nt
			return
		}()
		return
	}
}

// change the pointer of serviceDescriptors to new serviceDescriptors
func (c *Client) updateServiceDescriptorLocked(serviceDescriptors map[string]*desc.ServiceDescriptor) {
	if c.serviceDescriptors != nil {
		c.serviceDescriptors = nil
	}

	c.serviceDescriptors = &serviceDescriptors
}

func (c *Client) Invoke(ctx context.Context, contentType, serverName, methodName string, body []byte) ([]byte, error) {
	c.updateLock.RLock()
	defer c.updateLock.RUnlock()

	if c.serviceDescriptors == nil {
		return nil, errors.New("service descriptor not ready")
	}
	// get serviceDescriptor
	serviceDescriptor, ok := (*c.serviceDescriptors)[serverName]
	if !ok {
		return nil, errors.New("service not found")
	}
	// get method
	methodDescriptor := serviceDescriptor.FindMethodByName(methodName)
	if methodDescriptor == nil {
		return nil, errors.New("method not found")
	}
	inputType := methodDescriptor.GetInputType()
	dmInput, err := c.geInputMsg(inputType, contentType, body)
	if err != nil {
		return nil, err
	}

	// invoke grpcManager method
	ctx, cancelFunc := context.WithTimeout(ctx, c.invokeTimeout)
	defer cancelFunc()

	dmOutput, err := c.grpcStub.InvokeRpc(ctx, methodDescriptor, dmInput, grpc.WaitForReady(true))

	if err != nil {
		return nil, err
	}

	// marshal output
	rsp, err := c.getDynamicOutputMsg(dmOutput, contentType)
	if err != nil {
		return nil, err
	}

	return rsp, nil
}

func (c *Client) getDynamicOutputMsg(output proto.Message, contentType string) (rsp []byte, err error) {
	dmOutput := output.(*dynamic.Message)
	switch contentType {
	case "application/json":
		rsp, err = dmOutput.MarshalJSONPB(&jsonpb.Marshaler{
			EnumsAsInts:  true,
			EmitDefaults: true,
		})
	case "application/protobuf":
		rsp, err = dmOutput.Marshal()
	default:
		return nil, errors.New("unsupported content type")
	}

	return
}

func (c *Client) geInputMsg(inputType *desc.MessageDescriptor, contentType string, params []byte) (dmInput *dynamic.Message, err error) {
	dmInput = dynamic.NewMessage(inputType)
	switch contentType {
	case "application/json":
		err = dmInput.UnmarshalJSON(params)
		if err != nil {
			return nil, fmt.Errorf("unmarshalJSON failed, err: %v", err.Error())
		}
	case "application/protobuf":
		err = dmInput.Unmarshal(params)
		if err != nil {
			return nil, fmt.Errorf("unmarshalPROTO failed, err: %v", err.Error())
		}
	default:
		return nil, errors.New("unsupported content type")
	}

	return dmInput, err
}
