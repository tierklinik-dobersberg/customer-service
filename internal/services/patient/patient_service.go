package patient

import (
	"context"
	"fmt"

	"github.com/bufbuild/connect-go"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	"github.com/tierklinik-dobersberg/customer-service/internal/config"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
)

type PatientService struct {
	customerv1connect.UnimplementedPatientServiceHandler

	cfg        *config.Config
	repository repo.PatientBackend
}

func New(cfg *config.Config, repo repo.PatientBackend) *PatientService {
	return &PatientService{
		repository: repo,
		cfg:        cfg,
	}
}

func (svc *PatientService) GetPatientsByCustomer(ctx context.Context, req *connect.Request[customerv1.GetPatientsByCustomerRequest]) (*connect.Response[customerv1.GetPatientsByCustomerResponse], error) {
	patients, err := svc.repository.LookupPatientsByCustomerId(ctx, req.Msg.CustomerId)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&customerv1.GetPatientsByCustomerResponse{
		Patients: patients,
	}), nil
}

func (svc *PatientService) QueryPatients(ctx context.Context, req *connect.Request[customerv1.QueryPatientsRequests]) (*connect.Response[customerv1.QueryPatientsResponse], error) {
	patients, err := svc.repository.QueryPatients(ctx, req.Msg.Query)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&customerv1.QueryPatientsResponse{
		Patients: patients,
	}), nil
}

func (svc *PatientService) GetPatient(ctx context.Context, req *connect.Request[customerv1.GetPatientRequest]) (*connect.Response[customerv1.Patient], error) {
	var (
		patient *customerv1.Patient
		err     error
	)

	switch v := req.Msg.Kind.(type) {
	case *customerv1.GetPatientRequest_AnimalId:
		patient, err = svc.repository.LookupPatientById(ctx, v.AnimalId)
	case *customerv1.GetPatientRequest_AdditionalUniqueId:
		patient, err = svc.repository.LookupPatientByAdditionalUniqueId(ctx, v.AdditionalUniqueId)

	default:
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("unsupported value of GetPatientRequest.kind"))
	}

	if err != nil {
		return nil, err
	}

	return connect.NewResponse(patient), nil
}
