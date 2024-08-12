package importer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hashicorp/go-multierror"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
)

type Manager struct {
	dispatcher *Dispatcher
	differ     Differ
}

func NewManager(ctx context.Context, importer string, stream ImportStream) (*Manager, error) {
	dipatcher := NewDispatcher(ctx, importer, stream)

	mng := &Manager{
		dispatcher: dipatcher,
		differ:     DefaultDiffer,
	}

	mng.dispatcher.Start()

	slog.Info("sending request: start_session for importer", "identifier", importer)

	res := mng.dispatcher.Send(&customerv1.ImportSessionRequest{
		Message: &customerv1.ImportSessionRequest_StartSession{
			StartSession: &customerv1.StartSessionRequest{
				Importer: importer,
			},
		},
	})

	if res == nil {
		return nil, fmt.Errorf("stream already closed")
	}

	slog.Info("waiting for response")

	select {
	case msg := <-res:
		if msg.GetStartSession() == nil {
			return nil, fmt.Errorf("invalid response for start-session request")
		}

	case <-ctx.Done():
		return nil, ctx.Err()
	}

	slog.Info("import session created successfully")

	return mng, nil
}

func (mng *Manager) lookupCustomer(req *customerv1.LookupCustomerRequest) (*customerv1.ImportedCustomer, error) {
	res := <-mng.dispatcher.Send(&customerv1.ImportSessionRequest{
		Message: &customerv1.ImportSessionRequest_LookupCustomer{
			LookupCustomer: req,
		},
	})

	lookupResponse := res.GetLookupCustomer()
	if lookupResponse == nil {
		return nil, fmt.Errorf("invalid response type %T", res.Message)
	}

	if len(lookupResponse.MatchedCustomers) > 1 {
		return nil, fmt.Errorf("to many results")
	}

	if len(lookupResponse.MatchedCustomers) == 1 {
		return lookupResponse.MatchedCustomers[0], nil
	}

	return &customerv1.ImportedCustomer{
		Customer: &customerv1.Customer{},
		State:    &customerv1.ImportState{},
	}, nil
}

func (mng *Manager) upsertCustomer(ref string, importedCustomer *customerv1.ImportedCustomer, customer *customerv1.Customer) error {
	// generate a diff for the new customer
	diff, err := mng.differ.Diff(importedCustomer.Customer, customer)
	if err != nil {
		return fmt.Errorf("failed to prepare customer diff: %w", err)
	}

	// send an upsert request
	upsertResult := <-mng.dispatcher.Send(&customerv1.ImportSessionRequest{
		Message: &customerv1.ImportSessionRequest_UpsertCustomer{
			UpsertCustomer: &customerv1.UpsertCustomerRequest{
				InternalReference: ref,
				Id:                importedCustomer.Customer.Id,
				Updates:           diff,
			},
		},
	})

	if upsertError := upsertResult.GetUpsertError(); upsertError != nil {
		err := &multierror.Error{}

		for _, e := range upsertError.Errors {
			err.Errors = append(err.Errors, fmt.Errorf("%s on %s: %s", e.Operation.String(), e.Kind.String(), e.Error))
		}

		return fmt.Errorf("failed to upsert customer: %w", err)
	}

	return nil
}

func (mng *Manager) UpsertCustomerByRef(interalReference string, customer *customerv1.Customer) error {
	// first, lookup any existing customer
	importedCustomer, err := mng.lookupCustomer(&customerv1.LookupCustomerRequest{
		Query: &customerv1.CustomerQuery{
			Query: &customerv1.CustomerQuery_InternalReference{
				InternalReference: &customerv1.InternalReferenceQuery{
					Ref: interalReference,
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to lookup existing customer record: %w", err)
	}

	return mng.upsertCustomer(interalReference, importedCustomer, customer)
}

func (mng *Manager) Stop() error {
	mng.dispatcher.Stop()

	return nil
}
