package session

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/bufbuild/connect-go"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/customer-service/internal/attributes"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
)

type ImportStream = connect.BidiStream[customerv1.ImportSessionRequest, customerv1.ImportSessionResponse]

type ImportSession struct {
	stream   *ImportStream
	store    repo.Repo
	wg       sync.WaitGroup
	importer string

	sendQueue chan *customerv1.ImportSessionResponse

	upserts          atomic.Uint64
	attributeUpdates atomic.Uint64
	lookups          atomic.Uint64
}

func NewImportSession(stream *ImportStream, store repo.Repo) *ImportSession {
	return &ImportSession{
		stream:    stream,
		store:     store,
		sendQueue: make(chan *customerv1.ImportSessionResponse, 100),
	}
}

func (session *ImportSession) Handle(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// first, receive the session-start message
	msg, err := session.stream.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive session_start request: %w", err)
	}

	start, ok := msg.Message.(*customerv1.ImportSessionRequest_StartSession)
	if !ok {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("expected a session_start request"))
	}

	session.importer = start.StartSession.GetImporter()
	if session.importer == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid importer field in start_session request"))
	}

	if err := session.stream.Send(&customerv1.ImportSessionResponse{
		CorrelationId: msg.CorrelationId,
		Message:       &customerv1.ImportSessionResponse_StartSession{},
	}); err != nil {
		return fmt.Errorf("failed to send start_session response: %w", err)
	}

	session.wg.Add(1)
	go session.sendLoop(ctx)

	for {
		msg, err := session.stream.Receive()
		if err != nil {
			slog.ErrorContext(ctx, "failed to receive message", slog.Attr{
				Key:   "error",
				Value: slog.StringValue(err.Error()),
			})

			break
		}

		if _, ok := msg.Message.(*customerv1.ImportSessionRequest_Complete); ok {

			break
		}

		session.wg.Add(1)
		go session.handleMessage(ctx, msg)
	}

	close(session.sendQueue)
	session.wg.Wait()

	slog.Info("import session complete", "identifier", session.importer, "upserts", session.upserts.Load(), "lookups", session.lookups.Load(), "attribute-updates", session.attributeUpdates.Load())

	return nil
}

func (session *ImportSession) handleMessage(ctx context.Context, msg *customerv1.ImportSessionRequest) {
	defer session.wg.Done()

	switch v := msg.Message.(type) {
	case *customerv1.ImportSessionRequest_LookupCustomer:
		session.handleCustomerLookup(ctx, msg.CorrelationId, v)

	case *customerv1.ImportSessionRequest_UpsertCustomer:
		session.handleUpsert(ctx, msg.CorrelationId, v)

	default:
		slog.ErrorContext(ctx, "unsupported request message", slog.Attr{
			Key:   "type",
			Value: slog.StringValue(fmt.Sprintf("%T", v)),
		})
	}
}

func (session *ImportSession) handleCustomerLookup(ctx context.Context, correlationId string, msg *customerv1.ImportSessionRequest_LookupCustomer) {
	session.lookups.Add(1)

	// TODO(ppacher): handle the error gracefully
	results, _ := session.store.SearchQuery(ctx, msg.LookupCustomer.Query)

	res := &customerv1.ImportSessionResponse_LookupCustomer{
		LookupCustomer: &customerv1.LookupCustomerResponse{
			MatchedCustomers: make([]*customerv1.ImportedCustomer, len(results)),
		},
	}

	for idx, r := range results {
		res.LookupCustomer.MatchedCustomers[idx] = &customerv1.ImportedCustomer{
			Customer: r.Customer,
			State:    session.findImporterState(r.States),
		}
	}

	select {
	case session.sendQueue <- &customerv1.ImportSessionResponse{
		CorrelationId: correlationId,
		Message:       res,
	}:

	case <-ctx.Done():
		return
	}
}

func (session *ImportSession) handleUpsert(ctx context.Context, correlationId string, msg *customerv1.ImportSessionRequest_UpsertCustomer) {
	var (
		customer *customerv1.Customer
		states   []*customerv1.ImportState
		err      error
	)

	if msg.UpsertCustomer.GetId() != "" {
		customer, states, err = session.store.LookupCustomerById(ctx, msg.UpsertCustomer.Id)

		if err == nil {
			var unlock func()

			unlock, err = session.store.LockCustomer(ctx, customer.Id)
			defer unlock() // unlock is never nil
		}

		if err != nil {
			session.sendQueue <- &customerv1.ImportSessionResponse{
				CorrelationId: correlationId,
				Message: &customerv1.ImportSessionResponse_UpsertError{
					UpsertError: &customerv1.UpsertCustomerError{
						Errors: []*customerv1.AttributeUpdateError{
							{
								Error: err.Error(), // FIXME
							},
						},
					},
				},
			}
		}
	}

	am := attributes.NewManager(msg.UpsertCustomer.InternalReference, session.importer, customer, states, false)

	attrErrors := make([]*customerv1.AttributeUpdateError, 0)
	hasChanges := false

	for _, upd := range msg.UpsertCustomer.Updates {
		if err := am.ApplyUpdate(upd); err != nil {
			attrErrors = append(attrErrors, &customerv1.AttributeUpdateError{
				Kind:      upd.Kind,
				Operation: upd.Operation,
				Error:     err.Error(),
			})
		} else {
			session.attributeUpdates.Add(1)
			hasChanges = true
		}
	}

	if hasChanges {
		session.upserts.Add(1)
		slog.Info("storing customer", "customer", am.Customer.LastName)
		if err := session.store.StoreCustomer(ctx, am.Customer, am.States); err != nil {
			attrErrors = append(attrErrors, &customerv1.AttributeUpdateError{
				Error: err.Error(),
			})

			slog.ErrorContext(ctx, "failed to store customer", slog.Attr{
				Key:   "error",
				Value: slog.StringValue(err.Error()),
			})
		}
	}

	if len(attrErrors) > 0 {
		select {
		case session.sendQueue <- &customerv1.ImportSessionResponse{
			CorrelationId: correlationId,
			Message: &customerv1.ImportSessionResponse_UpsertError{
				UpsertError: &customerv1.UpsertCustomerError{
					Errors: attrErrors,
				},
			},
		}:

		case <-ctx.Done():
		}

		return
	}

	select {
	case session.sendQueue <- &customerv1.ImportSessionResponse{
		CorrelationId: correlationId,
		Message: &customerv1.ImportSessionResponse_UpsertSuccess{
			UpsertSuccess: &customerv1.UpsertCustomerSuccess{
				Id: am.Customer.Id,
			},
		},
	}:

	case <-ctx.Done():
	}
}

func (session *ImportSession) findImporterState(states []*customerv1.ImportState) *customerv1.ImportState {
	for _, s := range states {
		if s.Importer == session.importer {
			return s
		}
	}

	return nil
}

func (session *ImportSession) sendLoop(ctx context.Context) {
	defer session.wg.Done()

	for {
		select {
		case msg, ok := <-session.sendQueue:
			if !ok {
				return
			}
			if err := session.stream.Send(msg); err != nil {
				slog.ErrorContext(ctx, "failed to send response", slog.Attr{
					Key:   "error",
					Value: slog.StringValue(err.Error()),
				})
				return
			}

		case <-ctx.Done():
			return
		}
	}
}
