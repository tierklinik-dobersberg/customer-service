package repo

import (
	"context"
	"errors"

	"github.com/nyaruka/phonenumbers"
	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
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

	ListCustomers(ctx context.Context, pagination *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error)

	LookupCustomerById(ctx context.Context, id string) (*customerv1.Customer, []*customerv1.ImportState, error)
	LookupCustomerByRef(ctx context.Context, importer, ref string) (*customerv1.Customer, []*customerv1.ImportState, error)

	LookupCustomerByName(ctx context.Context, name string, pagination *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error)
	LookupCustomerByPhone(ctx context.Context, phone string, pagination *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error)
	LookupCustomerByMail(ctx context.Context, mail string, pagination *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error)
}

type Repo interface {
	Backend
	SingleQueryRunnger
	MultiQueryRunner
}

type SingleQueryRunnger interface {
	SearchQuery(ctx context.Context, query *customerv1.CustomerQuery, p *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error)
}

type MultiQueryRunner interface {
	SearchQueries(ctx context.Context, queries []*customerv1.CustomerQuery, p *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error)
}

type repo struct {
	Backend
}

func New(backend Backend) Repo {
	return &repo{
		Backend: backend,
	}
}

func (r *repo) SearchQueries(ctx context.Context, queries []*customerv1.CustomerQuery, p *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error) {
	if cap, ok := r.Backend.(MultiQueryRunner); ok {
		return cap.SearchQueries(ctx, queries, p)
	}

	// fallback to execute each query on it's own and apply pagination afterwards.
	// sorting does not correctly work in this situation
	var results []*customerv1.CustomerResponse
	for _, q := range queries {
		res, _, err := r.SearchQuery(ctx, q, nil) // skip pagination here
		if err != nil {
			return nil, 0, err
		}

		results = append(results, res...)
	}

	// remove duplicates
	seen := make(map[string]struct{}, len(results))

	cleanedResult := make([]*customerv1.CustomerResponse, 0, len(results))
	for _, c := range results {
		if _, ok := seen[c.Customer.Id]; !ok {
			cleanedResult = append(cleanedResult, c)
			seen[c.Customer.Id] = struct{}{}
		}
	}

	if p != nil && p.PageSize > 0 {
		return cleanedResult[p.PageSize*p.GetPage() : p.PageSize*(p.GetPage()+1)], len(cleanedResult), nil
	}

	return cleanedResult, len(cleanedResult), nil
}

func (r *repo) SearchQuery(ctx context.Context, query *customerv1.CustomerQuery, p *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error) {
	if cap, ok := r.Backend.(SingleQueryRunnger); ok {
		return cap.SearchQuery(ctx, query, p)
	}

	var customers []*customerv1.CustomerResponse
	var count int

	if query == nil || query.Query == nil {
		return r.Backend.ListCustomers(ctx, p)
	}

	switch v := query.Query.(type) {
	case *customerv1.CustomerQuery_Id:
		c, states, err := r.LookupCustomerById(ctx, v.Id)
		if err != nil && !errors.Is(err, ErrCustomerNotFound) {
			return nil, 0, err
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
			return nil, 0, err
		}

		if c != nil {
			customers = append(customers, &customerv1.CustomerResponse{
				Customer: c,
				States:   states,
			})
		}

	case *customerv1.CustomerQuery_Name:
		results, c, err := r.LookupCustomerByName(ctx, v.Name.LastName, p)
		if err != nil && !errors.Is(err, ErrCustomerNotFound) {
			return nil, 0, err
		}

		customers = append(customers, results...)
		count = c

	case *customerv1.CustomerQuery_PhoneNumber:
		phone := v.PhoneNumber
		parsed, err := phonenumbers.Parse(v.PhoneNumber, "AT")
		if err == nil {
			phone = phonenumbers.Format(parsed, phonenumbers.INTERNATIONAL)
		}

		results, c, err := r.LookupCustomerByPhone(ctx, phone, p)
		if err != nil {
			return nil, 0, err
		}

		customers = append(customers, results...)
		count = c

	case *customerv1.CustomerQuery_EmailAddress:
		results, c, err := r.LookupCustomerByMail(ctx, v.EmailAddress, p)
		if err != nil {
			return nil, 0, err
		}

		customers = append(customers, results...)
		count = c
	}

	return customers, count, nil
}
