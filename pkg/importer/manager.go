package importer

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-multierror"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type Manager struct {
	dispatcher *Dispatcher
}

func NewManager(ctx context.Context, importer string, stream ImportStream) (*Manager, error) {
	dipatcher := NewDispatcher(ctx, importer, stream)

	mng := &Manager{
		dispatcher: dipatcher,
	}

	mng.dispatcher.Start()

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

	select {
	case msg := <-res:
		if msg.GetStartSession() == nil {
			return nil, fmt.Errorf("invalid response for start-session request")
		}

	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return mng, nil
}

func (mng *Manager) UpsertPatient(customerRef string, patient *customerv1.Patient) error {
	// lookup the customer
	lookupResult := <-mng.dispatcher.Send(&customerv1.ImportSessionRequest{
		Message: &customerv1.ImportSessionRequest_LookupCustomer{
			LookupCustomer: &customerv1.LookupCustomerRequest{
				Query: &customerv1.CustomerQuery{
					Query: &customerv1.CustomerQuery_InternalReference{
						InternalReference: &customerv1.InternalReferenceQuery{
							Importer: mng.dispatcher.importer,
							Ref:      customerRef,
						},
					},
				},
			},
		},
	})

	if lerr := lookupResult.GetError(); lerr != nil {
		err := &multierror.Error{}

		for _, e := range lerr.Error {
			err.Errors = append(err.Errors, errors.New(e))
		}

		return fmt.Errorf("failed to lookup customer: %w", err)
	}

	res := lookupResult.GetLookupCustomer()
	if res == nil || len(res.MatchedCustomers) == 0 {
		return fmt.Errorf("failed to lookup customer, res is nil or no results found")
	}

	customer := res.MatchedCustomers[0].Customer

	patient.CustomerId = customer.Id
	patient.Importer = mng.dispatcher.importer

	upsertResult := <-mng.dispatcher.Send(&customerv1.ImportSessionRequest{
		Message: &customerv1.ImportSessionRequest_UpsertPatient{
			UpsertPatient: &customerv1.UpsertPatientRequest{
				Patient: patient,
			},
		},
	})

	if upsertError := upsertResult.GetError(); upsertError != nil {
		err := &multierror.Error{}

		for _, e := range upsertError.Error {
			err.Errors = append(err.Errors, errors.New(e))
		}

		return fmt.Errorf("failed to upsert patient: %w", err)
	}

	return nil
}

func (mng *Manager) upsertCustomer(ref string, customer *customerv1.Customer, extraData *structpb.Struct) error {
	// send an upsert request
	upsertResult := <-mng.dispatcher.Send(&customerv1.ImportSessionRequest{
		Message: &customerv1.ImportSessionRequest_UpsertCustomer{
			UpsertCustomer: &customerv1.UpsertCustomerRequest{
				InternalReference: ref,
				Customer:          customer,
				ExtraData:         extraData,
			},
		},
	})

	if upsertError := upsertResult.GetError(); upsertError != nil {
		err := &multierror.Error{}

		for _, e := range upsertError.Error {
			err.Errors = append(err.Errors, errors.New(e))
		}

		return fmt.Errorf("failed to upsert customer: %w", err)
	}

	return nil
}

func (mng *Manager) UpsertCustomerByRef(interalReference string, customer *customerv1.Customer, extra map[string]interface{}) error {
	extraPb, err := structpb.NewStruct(extra)
	if err != nil {
		return fmt.Errorf("invalid extra data: %w", err)
	}

	return mng.upsertCustomer(interalReference, customer, extraPb)
}

func (mng *Manager) Stop() error {
	mng.dispatcher.Stop()

	return nil
}
