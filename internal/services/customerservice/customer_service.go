package customerservice

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/bufbuild/connect-go"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"github.com/tierklinik-dobersberg/customer-service/internal/session"
)

type CustomerService struct {
	customerv1connect.UnimplementedCustomerServiceHandler

	repo     repo.Repo
	resolver session.PriorityResolver
}

func New(repo repo.Repo, resolver session.PriorityResolver) *CustomerService {
	return &CustomerService{
		repo:     repo,
		resolver: resolver,
	}
}

func (svc *CustomerService) SearchCustomerStream(ctx context.Context, stream *connect.BidiStream[customerv1.SearchCustomerRequest, customerv1.SearchCustomerResponse]) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg      sync.WaitGroup
		lastErr error
	)

	for {
		msg, err := stream.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			slog.ErrorContext(ctx, "failed to receive from stream", slog.Any("error", err.Error()))
			lastErr = err
			break
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			queries := msg.Queries
			// if no queries are given we're searching for all customers using an empty query
			if len(queries) == 0 {
				queries = []*customerv1.CustomerQuery{}
			}

			var (
				response []*customerv1.CustomerResponse
				set      = make(map[string]struct{})
			)

			for _, query := range queries {
				if ctx.Err() != nil {
					return
				}

				res, err := svc.repo.SearchQuery(ctx, query)
				if err != nil {
					slog.ErrorContext(ctx, "failed to search customers", slog.Any("error", err.Error()))

					// send an empty response
					if err := stream.Send(&customerv1.SearchCustomerResponse{
						CorrelationId: msg.CorrelationId,
					}); err != nil {
						slog.ErrorContext(ctx, "failed to send stream message", slog.Any("error", err.Error()))
					}

					continue
				}

				for _, c := range res {
					if _, ok := set[c.Customer.Id]; !ok {
						response = append(response, c)
						set[c.Customer.Id] = struct{}{}
					}
				}
			}

			if ctx.Err() != nil {
				return
			}

			if err := stream.Send(&customerv1.SearchCustomerResponse{
				Results:       response,
				CorrelationId: msg.CorrelationId,
			}); err != nil {
				slog.ErrorContext(ctx, "failed to send stream message", slog.Any("error", err.Error()))
			}
		}()
	}

	wg.Wait()

	return lastErr
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
	var (
		customer *customerv1.Customer
		states   []*customerv1.ImportState
		err      error
	)

	if id := req.Msg.GetCustomer().GetId(); id != "" {
		customer, states, err = svc.repo.LookupCustomerById(ctx, id)
		if err != nil {
			if errors.Is(err, repo.ErrCustomerNotFound) {
				return nil, connect.NewError(connect.CodeNotFound, err)
			}

			return nil, err
		}
	}

	p := session.NewPatcher("user", "ref", svc.resolver, customer, states)

	if err := p.Apply(req.Msg.Customer); err != nil {
		return nil, err
	}

	if err := svc.repo.StoreCustomer(ctx, p.Result, p.States); err != nil {
		return nil, err
	}

	return connect.NewResponse(&customerv1.UpdateCustomerResponse{
		Response: &customerv1.CustomerResponse{
			Customer: p.Result,
			States:   p.States,
		},
	}), nil
}
