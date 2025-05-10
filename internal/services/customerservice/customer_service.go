package customerservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/bufbuild/connect-go"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	"github.com/tierklinik-dobersberg/customer-service/internal/config"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"github.com/tierklinik-dobersberg/customer-service/internal/session"
)

type CustomerService struct {
	customerv1connect.UnimplementedCustomerServiceHandler

	customerRepository repo.CustomerRepository
	config             *config.Config
	resolver           session.PriorityResolver
}

func New(config *config.Config, customerRepo repo.CustomerRepository, resolver session.PriorityResolver) *CustomerService {
	return &CustomerService{
		config:             config,
		customerRepository: customerRepo,
		resolver:           resolver,
	}
}

func (svc *CustomerService) SearchCustomerStream(ctx context.Context, stream *connect.BidiStream[customerv1.SearchCustomerRequest, customerv1.SearchCustomerResponse]) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg      sync.WaitGroup
		lastErr error
		pending atomic.Int64
		l       sync.Mutex
	)

	for {
		msg, err := stream.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			cancel()

			slog.ErrorContext(ctx, "failed to receive from stream", slog.Any("error", err.Error()))
			lastErr = err
			break
		}

		pending.Add(1)
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer pending.Add(-1)

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
					slog.InfoContext(ctx, "aborting queries, context cancelled")
					return
				}

				res, _, err := svc.customerRepository.PerformCustomerQuery(ctx, query, nil)
				if err != nil {
					slog.ErrorContext(ctx, "failed to search customers", slog.Any("error", err.Error()))

					l.Lock()
					defer l.Unlock()

					// send an empty response
					if err := stream.Send(&customerv1.SearchCustomerResponse{
						CorrelationId: msg.CorrelationId,
					}); err != nil {
						slog.ErrorContext(ctx, "failed to send empty stream message", slog.Any("error", err.Error()))
					}

					return
				}

				for _, c := range res {
					if _, ok := set[c.Customer.Id]; !ok {
						response = append(response, c)
						set[c.Customer.Id] = struct{}{}
					}
				}
			}

			l.Lock()
			defer l.Unlock()

			if err := stream.Send(&customerv1.SearchCustomerResponse{
				Results:       response,
				CorrelationId: msg.CorrelationId,
				TotalResults:  int64(len(response)),
			}); err != nil {
				slog.ErrorContext(ctx, "failed to send stream message", slog.Any("error", err.Error()))
			}
		}()
	}

	slog.InfoContext(ctx, "waiting for go-routines to finish", slog.Any("count", pending.Load()))
	wg.Wait()
	slog.InfoContext(ctx, "go-routines finsihed, response done")

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

	customers, count, err := svc.customerRepository.PerformCustomerQueries(ctx, msg.Msg.Queries, msg.Msg.Pagination)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&customerv1.SearchCustomerResponse{
		Results:      customers,
		TotalResults: int64(count),
	}), nil
}

func (svc *CustomerService) UpdateCustomer(ctx context.Context, req *connect.Request[customerv1.UpdateCustomerRequest]) (*connect.Response[customerv1.UpdateCustomerResponse], error) {
	var (
		customer *customerv1.Customer
		states   []*customerv1.ImportState
		err      error
	)

	if id := req.Msg.GetCustomer().GetId(); id != "" {
		customer, states, err = svc.customerRepository.LookupCustomerById(ctx, id)
		if err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				return nil, connect.NewError(connect.CodeNotFound, err)
			}

			return nil, err
		}
	} else if ph := req.Msg.GetCustomer().GetPhoneNumbers(); len(ph) > 0 {
		for _, p := range ph {
			customers, _, err := svc.customerRepository.LookupCustomerByPhone(ctx, p, nil)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to search customers: %w", err))
			}

			if len(customers) == 1 {
				customer = customers[0].Customer
				states = customers[0].States
			}
		}
	}

	p := session.NewCustomerPatcher("user", "ref", svc.config.Country, svc.resolver, customer, states)

	if err := p.Apply(req.Msg.Customer); err != nil {
		return nil, err
	}

	if err := svc.customerRepository.StoreCustomer(ctx, p.Result, p.States); err != nil {
		return nil, err
	}

	return connect.NewResponse(&customerv1.UpdateCustomerResponse{
		Response: &customerv1.CustomerResponse{
			Customer: p.Result,
			States:   p.States,
		},
	}), nil
}
