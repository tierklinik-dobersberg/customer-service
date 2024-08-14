package customerservice

import (
	"context"
	"errors"

	"github.com/bufbuild/connect-go"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	"github.com/tierklinik-dobersberg/customer-service/internal/attributes"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
)

type CustomerService struct {
	customerv1connect.UnimplementedCustomerServiceHandler

	repo repo.Repo
}

func New(repo repo.Repo) *CustomerService {
	return &CustomerService{
		repo: repo,
	}
}

func (svc *CustomerService) SearchCustomer(ctx context.Context, msg *connect.Request[customerv1.SearchCustomerRequest]) (*connect.Response[customerv1.SearchCustomerResponse], error) {
	var (
		customers []*customerv1.CustomerResponse
	)

	// if no queries are given we're searching for all customers using an empty query
	if len(msg.Msg.Queries) == 0 {
		msg.Msg.Queries = append(msg.Msg.Queries, &customerv1.CustomerQuery{})
	}

	for _, query := range msg.Msg.Queries {
		results, err := svc.repo.SearchQuery(ctx, query)
		if err != nil {
			return nil, err
		}

		customers = append(customers, results...)
	}

	return connect.NewResponse(&customerv1.SearchCustomerResponse{
		Results: customers,
	}), nil
}

func (svc *CustomerService) UpdateCustomer(ctx context.Context, req *connect.Request[customerv1.UpdateCustomerRequest]) (*connect.Response[customerv1.UpdateCustomerResponse], error) {
	customer, states, err := svc.repo.LookupCustomerById(ctx, req.Msg.Id)
	if err != nil {
		if errors.Is(err, repo.ErrCustomerNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	am := attributes.NewManager("", "user", customer, states, true)

	hasChanges := false

	for _, upd := range req.Msg.Updates {
		if err := am.ApplyUpdate(upd); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}

		hasChanges = true
	}

	if hasChanges {
		if err := svc.repo.StoreCustomer(ctx, am.Customer, am.States); err != nil {
			return nil, err
		}
	}

	return connect.NewResponse(&customerv1.UpdateCustomerResponse{
		Response: &customerv1.CustomerResponse{
			Customer: am.Customer,
			States:   am.States,
		},
	}), nil
}
