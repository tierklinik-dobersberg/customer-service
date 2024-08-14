package repo

import (
	"context"
	"errors"

	"github.com/nyaruka/phonenumbers"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"google.golang.org/protobuf/proto"
)

func Clone[T proto.Message](a T) T {
	return proto.Clone(a).(T)
}

type Backend interface {
	// StoreCustomer upserts a customer record into the database.
	StoreCustomer(ctx context.Context, customer *customerv1.Customer, states []*customerv1.ImportState) error

	// LockCustomer locks a customer record.
	LockCustomer(ctx context.Context, id string) (func(), error)

	// Lookup methds

	ListCustomers(ctx context.Context) ([]*customerv1.CustomerResponse, error)

	LookupCustomerById(ctx context.Context, id string) (*customerv1.Customer, []*customerv1.ImportState, error)
	LookupCustomerByRef(ctx context.Context, importer, ref string) (*customerv1.Customer, []*customerv1.ImportState, error)

	LookupCustomerByName(ctx context.Context, name string) ([]*customerv1.CustomerResponse, error)
	LookupCustomerByPhone(ctx context.Context, phone string) ([]*customerv1.CustomerResponse, error)
	LookupCustomerByMail(ctx context.Context, mail string) ([]*customerv1.CustomerResponse, error)
}

type Repo interface {
	Backend

	SearchQuery(ctx context.Context, query *customerv1.CustomerQuery) ([]*customerv1.CustomerResponse, error)
}

type repo struct {
	Backend
}

func New(backend Backend) Repo {
	return &repo{
		Backend: backend,
	}
}

func (r *repo) SearchQuery(ctx context.Context, query *customerv1.CustomerQuery) ([]*customerv1.CustomerResponse, error) {
	var customers []*customerv1.CustomerResponse

	if query == nil || query.Query == nil {
		return r.Backend.ListCustomers(ctx)
	}

	switch v := query.Query.(type) {
	case *customerv1.CustomerQuery_Id:
		c, states, err := r.LookupCustomerById(ctx, v.Id)
		if err != nil && !errors.Is(err, ErrCustomerNotFound) {
			return nil, err
		}

		if c != nil {
			customers = append(customers, &customerv1.CustomerResponse{
				Customer: c,
				States:   states,
			})
		}

	case *customerv1.CustomerQuery_InternalReference:
		c, states, err := r.LookupCustomerByRef(ctx, v.InternalReference.Importer, v.InternalReference.Ref)
		if err != nil && !errors.Is(err, ErrCustomerNotFound) {
			return nil, err
		}

		if c != nil {
			customers = append(customers, &customerv1.CustomerResponse{
				Customer: c,
				States:   states,
			})
		}

	case *customerv1.CustomerQuery_Name:
		results, err := r.LookupCustomerByName(ctx, v.Name.LastName)
		if err != nil && !errors.Is(err, ErrCustomerNotFound) {
			return nil, err
		}

		customers = append(customers, results...)

	case *customerv1.CustomerQuery_PhoneNumber:
		p := v.PhoneNumber
		parsed, err := phonenumbers.Parse(v.PhoneNumber, "AT")
		if err == nil {
			p = phonenumbers.Format(parsed, phonenumbers.INTERNATIONAL)
		}

		results, err := r.LookupCustomerByPhone(ctx, p)
		if err != nil {
			return nil, err
		}

		customers = append(customers, results...)

	case *customerv1.CustomerQuery_EmailAddress:
		results, err := r.LookupCustomerByMail(ctx, v.EmailAddress)
		if err != nil {
			return nil, err
		}

		customers = append(customers, results...)
	}

	return customers, nil
}
