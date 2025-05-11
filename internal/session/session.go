package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/bufbuild/connect-go"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ImportStream = connect.BidiStream[customerv1.ImportSessionRequest, customerv1.ImportSessionResponse]

type ImportSession struct {
	stream             *ImportStream
	customerRepository repo.CustomerRepository
	patientRepository  repo.PatientBackend
	wg                 sync.WaitGroup
	importer           string
	resolver           PriorityResolver
	country            string

	sendQueue chan *customerv1.ImportSessionResponse

	upserts          atomic.Uint64
	attributeUpdates atomic.Uint64
	lookups          atomic.Uint64
}

func NewImportSession(country string, stream *ImportStream, customerRepo repo.CustomerRepository, patientRepo repo.PatientBackend, resolver PriorityResolver) *ImportSession {
	return &ImportSession{
		resolver:           resolver,
		stream:             stream,
		country:            country,
		customerRepository: customerRepo,
		patientRepository:  patientRepo,
		sendQueue:          make(chan *customerv1.ImportSessionResponse, 100),
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
		if err := session.handleCustomerUpsert(ctx, msg.CorrelationId, v); err != nil {
			session.sendError(ctx, msg.CorrelationId, err)
		}

	case *customerv1.ImportSessionRequest_UpsertPatient:
		if err := session.handlePatientUpsert(ctx, msg.CorrelationId, v); err != nil {
			session.sendError(ctx, msg.CorrelationId, err)
		}

	default:
		slog.ErrorContext(ctx, "unsupported request message", slog.Attr{
			Key:   "type",
			Value: slog.StringValue(fmt.Sprintf("%T", v)),
		})
	}
}

func (session *ImportSession) sendError(ctx context.Context, id string, err error) {
	select {
	case session.sendQueue <- &customerv1.ImportSessionResponse{
		CorrelationId: id,
		Message: &customerv1.ImportSessionResponse_Error{
			Error: &customerv1.Error{
				Error: []string{err.Error()},
			},
		},
	}:
	case <-ctx.Done():
	}
}

func (session *ImportSession) handleCustomerLookup(ctx context.Context, correlationId string, msg *customerv1.ImportSessionRequest_LookupCustomer) {
	session.lookups.Add(1)

	if v := msg.LookupCustomer.GetQuery().GetInternalReference(); v != nil && v.Importer == "" {
		v.Importer = session.importer
	}

	results, _, err := session.customerRepository.PerformCustomerQuery(ctx, msg.LookupCustomer.Query, nil)
	if err != nil && !errors.Is(err, repo.ErrNotFound) {
		slog.ErrorContext(ctx, "failed to search customers", slog.Any("error", err.Error()))
	}

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

func (session *ImportSession) handleCustomerUpsert(ctx context.Context, correlationId string, msg *customerv1.ImportSessionRequest_UpsertCustomer) error {
	var (
		customer *customerv1.Customer
		states   []*customerv1.ImportState
		err      error
	)

	if msg.UpsertCustomer.InternalReference != "" {
		customer, states, err = session.customerRepository.LookupCustomerByRef(ctx, session.importer, msg.UpsertCustomer.InternalReference)
		if err != nil && !errors.Is(err, repo.ErrNotFound) {
			return err
		}
	}

	// try to find by phone number
	if customer == nil {
		for _, phone := range msg.UpsertCustomer.Customer.PhoneNumbers {
			res, _, err := session.customerRepository.LookupCustomerByPhone(ctx, phone, nil)
			if err != nil {
				return err
			}

			if len(res) == 1 {
				customer = res[0].Customer
				states = res[0].States
				break
			}
		}
	}

	if customer != nil && customer.Id != "" {
		unlock, err := session.customerRepository.LockCustomer(ctx, customer.Id)
		if err != nil {
			return err
		}

		defer unlock()
	}

	p := NewCustomerPatcher(session.importer, msg.UpsertCustomer.InternalReference, session.country, session.resolver, customer, states)

	if err := p.Apply(msg.UpsertCustomer.GetCustomer()); err != nil {
		return fmt.Errorf("failed to apply updates: %w", err)
	}

	if err := session.customerRepository.StoreCustomer(ctx, p.Result, p.States); err != nil {
		return fmt.Errorf("failed to store customer: %w", err)
	}

	select {
	case session.sendQueue <- &customerv1.ImportSessionResponse{
		CorrelationId: correlationId,
		Message: &customerv1.ImportSessionResponse_UpsertCustomerSuccess{
			UpsertCustomerSuccess: &customerv1.UpsertCustomerSuccess{
				Id: p.Result.Id,
			},
		},
	}:
	case <-ctx.Done():
	}

	return nil
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

func (session *ImportSession) handlePatientUpsert(ctx context.Context, correlationId string, msg *customerv1.ImportSessionRequest_UpsertPatient) error {
	var (
		existing *customerv1.Patient
		err      error
	)

	record := proto.Clone(msg.UpsertPatient.Patient).(*customerv1.Patient)

	ref := record.GetInternalReference()
	if ref == "" {
		return fmt.Errorf("missing internal patient reference")
	}

	if record.GetCustomerId() == "" {
		return fmt.Errorf("missing customer id reference")
	}

	if record.PatientName == "" {
		return fmt.Errorf("missing patient name")
	}

	existing, err = session.patientRepository.LookupPatientByRef(ctx, session.importer, ref)
	if err != nil && !errors.Is(err, repo.ErrNotFound) {
		return err
	}

	if existing != nil && existing.PatientId != "" {
		unlock, err := session.patientRepository.LockPatient(ctx, existing.PatientId)
		if err != nil {
			return err
		}

		defer unlock()

		record.PatientId = existing.PatientId
		record.FirstSeen = existing.FirstSeen
	} else {
		record.FirstSeen = timestamppb.Now()
	}

	record.LastUpdated = timestamppb.Now()
	record.Importer = session.importer

	storedPatient, err := session.patientRepository.StorePatient(ctx, record)
	if err != nil {
		return fmt.Errorf("failed to store customer: %w", err)
	}

	select {
	case session.sendQueue <- &customerv1.ImportSessionResponse{
		CorrelationId: correlationId,
		Message: &customerv1.ImportSessionResponse_UpsertPatientSuccess{
			UpsertPatientSuccess: &customerv1.UpsertPatientSuccess{
				Id: storedPatient.PatientId,
			},
		},
	}:
	case <-ctx.Done():
	}

	return nil
}
