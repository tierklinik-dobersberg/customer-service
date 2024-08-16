package importer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
)

type (
	ImportStream interface {
		Receive() (*customerv1.ImportSessionResponse, error)
		Send(*customerv1.ImportSessionRequest) error
		CloseRequest() error
		CloseResponse() error
	}

	Dispatcher struct {
		ctx       context.Context
		importer  string
		stream    ImportStream
		wg        sync.WaitGroup
		sendQueue chan *customerv1.ImportSessionRequest

		closed atomic.Bool

		cancelSendLoop    func()
		cancelReceiveLoop func()

		l           sync.Mutex
		responseMap map[string]chan<- *customerv1.ImportSessionResponse
	}
)

func NewDispatcher(ctx context.Context, importer string, stream ImportStream) *Dispatcher {
	return &Dispatcher{
		ctx:         ctx,
		importer:    importer,
		stream:      stream,
		sendQueue:   make(chan *customerv1.ImportSessionRequest, 100),
		responseMap: make(map[string]chan<- *customerv1.ImportSessionResponse, 100),
	}
}

func (mng *Dispatcher) Start() {
	mng.wg.Add(2)

	var (
		sendCtx    context.Context
		receiveCtx context.Context
	)

	receiveCtx, mng.cancelReceiveLoop = context.WithCancel(mng.ctx)
	sendCtx, mng.cancelSendLoop = context.WithCancel(mng.ctx)

	go mng.receiveLoop(receiveCtx)
	go mng.sendLoop(sendCtx)
}

func (mng *Dispatcher) Stop() {
	mng.closed.Store(true)

	mng.sendQueue <- &customerv1.ImportSessionRequest{
		Message: &customerv1.ImportSessionRequest_Complete{},
	}

	mng.cancelReceiveLoop()

	mng.wg.Wait()
}

func (mng *Dispatcher) Send(req *customerv1.ImportSessionRequest) <-chan *customerv1.ImportSessionResponse {
	ch := make(chan *customerv1.ImportSessionResponse, 1)

	id := GenerateCorrelationId(32)

	req.CorrelationId = id

	mng.l.Lock()
	mng.responseMap[id] = ch
	mng.l.Unlock()

	select {
	case mng.sendQueue <- req:
	case <-mng.ctx.Done():
		return nil
	}

	return ch
}

func (mng *Dispatcher) sendLoop(ctx context.Context) {
	defer mng.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-mng.sendQueue:
			if err := mng.stream.Send(msg); err != nil {

				slog.ErrorContext(ctx, "failed to send message to import stream", slog.Attr{
					Key:   "error",
					Value: slog.StringValue(err.Error()),
				})

				return
			}

			if _, ok := msg.Message.(*customerv1.ImportSessionRequest_Complete); ok {
				if err := mng.stream.CloseRequest(); err != nil {
					slog.ErrorContext(ctx, "failed to close request stream", slog.Attr{
						Key:   "error",
						Value: slog.StringValue(err.Error()),
					})
				}

				return
			}
		}
	}
}

func (mng *Dispatcher) receiveLoop(ctx context.Context) {
	defer mng.wg.Done()

	for {
		if ctx.Err() != nil {
			break
		}

		res, err := mng.stream.Receive()
		if err != nil {
			if mng.closed.Load() && errors.Is(err, io.EOF) {
				return
			}

			slog.ErrorContext(ctx, "failed to receive from import stream", slog.Attr{
				Key:   "error",
				Value: slog.StringValue(err.Error()),
			})
			break
		}

		switch v := res.Message.(type) {
		case *customerv1.ImportSessionResponse_StartSession:
		case *customerv1.ImportSessionResponse_Error:
		case *customerv1.ImportSessionResponse_UpsertSuccess:
		case *customerv1.ImportSessionResponse_LookupCustomer:

		default:
			slog.ErrorContext(ctx, "invalid or unsupported stream response type", slog.Attr{
				Key: "type",
				Value: slog.StringValue(
					fmt.Sprintf("%T", v),
				),
			})
		}

		mng.wg.Add(1)
		go func() {
			defer mng.wg.Done()

			mng.l.Lock()
			ch, ok := mng.responseMap[res.CorrelationId]
			delete(mng.responseMap, res.CorrelationId)
			mng.l.Unlock()

			if ok {
				select {
				case ch <- res:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}
