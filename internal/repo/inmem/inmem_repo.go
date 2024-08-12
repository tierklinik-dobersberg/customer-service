package inmem

import (
	"context"
	"strings"
	"sync"

	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"github.com/tierklinik-dobersberg/customer-service/pkg/importer"
)

type Repository struct {
	l sync.RWMutex

	customers map[string]*customerv1.Customer
	states    map[string][]*customerv1.ImportState

	locks map[string]string
}

func New() *Repository {
	return &Repository{
		customers: make(map[string]*customerv1.Customer),
		states:    make(map[string][]*customerv1.ImportState),
		locks:     make(map[string]string),
	}
}

func (r *Repository) LockCustomer(ctx context.Context, id string) (func(), error) {
	r.l.Lock()
	defer r.l.Unlock()

	if _, ok := r.locks[id]; ok {
		return func() {}, repo.ErrCustomerLocked
	}

	lockId := importer.GenerateCorrelationId(32)
	r.locks[id] = lockId

	return func() {
		r.l.Lock()
		defer r.l.Unlock()

		storedLockId := r.locks[id]
		if storedLockId != lockId {
			panic("customer locks are invalid")
		}

		delete(r.locks, id)
	}, nil
}

func (r *Repository) StoreCustomer(ctx context.Context, customer *customerv1.Customer, states []*customerv1.ImportState) error {
	r.l.Lock()
	defer r.l.Unlock()

	if customer.Id == "" {
		customer.Id = importer.GenerateCorrelationId(32)
	}

	r.customers[customer.Id] = customer
	r.states[customer.Id] = states

	return nil
}

func (r *Repository) LookupCustomerByRef(ctx context.Context, importer string, internalRef string) (*customerv1.Customer, []*customerv1.ImportState, error) {
	r.l.RLock()
	defer r.l.RUnlock()

	var (
		existingCustomer *customerv1.Customer
	)

	for customerId, states := range r.states {
		for _, state := range states {
			if state.Importer == importer && state.InternalReference == internalRef {
				existingCustomer = r.customers[customerId]
				break
			}
		}
	}

	if existingCustomer == nil {
		return nil, nil, repo.ErrCustomerNotFound
	}

	customerClone := repo.Clone(existingCustomer)

	return customerClone, r.cloneCustomerStates(customerClone.Id), nil
}

func (r *Repository) LookupCustomerById(ctx context.Context, id string) (*customerv1.Customer, []*customerv1.ImportState, error) {
	r.l.RLock()
	defer r.l.RUnlock()

	customer, ok := r.customers[id]
	if !ok {
		return nil, nil, repo.ErrCustomerNotFound
	}

	customerClone := repo.Clone(customer)

	return customerClone, r.cloneCustomerStates(customerClone.Id), nil
}

func (r *Repository) LookupCustomerByMail(ctx context.Context, mail string) ([]*customerv1.CustomerResponse, error) {
	var results []*customerv1.CustomerResponse

	r.l.RLock()
	defer r.l.RUnlock()

	for _, customer := range r.customers {
		for _, m := range customer.EmailAddresses {
			if m == mail {
				results = append(results, &customerv1.CustomerResponse{
					Customer: repo.Clone(customer),
					States:   r.cloneCustomerStates(customer.Id),
				})
			}
		}
	}

	return results, nil
}

func (r *Repository) LookupCustomerByPhone(ctx context.Context, phone string) ([]*customerv1.CustomerResponse, error) {
	var results []*customerv1.CustomerResponse

	r.l.RLock()
	defer r.l.RUnlock()

	for _, customer := range r.customers {
		for _, m := range customer.PhoneNumbers {
			if m == phone {
				results = append(results, &customerv1.CustomerResponse{
					Customer: repo.Clone(customer),
					States:   r.cloneCustomerStates(customer.Id),
				})
			}
		}
	}

	return results, nil
}

func (r *Repository) LookupCustomerByName(ctx context.Context, name string) ([]*customerv1.CustomerResponse, error) {
	var results []*customerv1.CustomerResponse

	r.l.RLock()
	defer r.l.RUnlock()

	for _, customer := range r.customers {
		customerName := strings.ToLower(customer.LastName + " " + customer.FirstName)
		if strings.Contains(customerName, strings.ToLower(name)) {
			results = append(results, &customerv1.CustomerResponse{
				Customer: repo.Clone(customer),
				States:   r.cloneCustomerStates(customer.Id),
			})
		}
	}

	return results, nil
}

func (r *Repository) cloneCustomerStates(id string) []*customerv1.ImportState {
	states := make([]*customerv1.ImportState, len(r.states[id]))

	for idx, s := range r.states[id] {
		states[idx] = repo.Clone(s)
	}

	return states
}
