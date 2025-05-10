package importservice

import (
	"context"

	"github.com/bufbuild/connect-go"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/customer-service/internal/config"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"github.com/tierklinik-dobersberg/customer-service/internal/session"
)

type ImportService struct {
	config             *config.Config
	customerRepository repo.CustomerRepository
	patientRepository  repo.PatientBackend
	resolver           session.PriorityResolver

	//customerv1connect.UnimplementedCustomerImportServiceHandler
}

func NewImportService(config *config.Config, customerRepo repo.CustomerRepository, patientRepo repo.PatientBackend, resolver session.PriorityResolver) *ImportService {
	return &ImportService{
		config:             config,
		customerRepository: customerRepo,
		resolver:           resolver,
		patientRepository:  patientRepo,
	}
}

func (svc *ImportService) ImportSession(ctx context.Context, stream *connect.BidiStream[customerv1.ImportSessionRequest, customerv1.ImportSessionResponse]) error {
	// create a new import session hand start handling customer updates.
	session := session.NewImportSession(svc.config.Country, stream, svc.customerRepository, svc.patientRepository, svc.resolver)

	return session.Handle(ctx)
}
