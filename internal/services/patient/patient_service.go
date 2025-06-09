package patient

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/bufbuild/connect-go"
	"github.com/mennanov/fmutils"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	treatmentv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/treatment/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/discovery/wellknown"
	"github.com/tierklinik-dobersberg/customer-service/internal/config"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"google.golang.org/protobuf/types/known/emptypb"
)

type PatientService struct {
	customerv1connect.UnimplementedPatientServiceHandler

	cfg        *config.Config
	clients    wellknown.Clients
	repository repo.PatientBackend
}

func New(cfg *config.Config, repo repo.PatientBackend, clients wellknown.Clients) *PatientService {
	return &PatientService{
		repository: repo,
		cfg:        cfg,
		clients:    clients,
	}
}

func (svc *PatientService) GetPatientsByCustomer(ctx context.Context, req *connect.Request[customerv1.GetPatientsByCustomerRequest]) (*connect.Response[customerv1.GetPatientsByCustomerResponse], error) {
	patients, err := svc.repository.LookupPatientsByCustomerId(ctx, req.Msg.CustomerId)
	if err != nil {
		return nil, err
	}

	speciesNames := make(map[string]struct{})
	for _, p := range patients {
		if p.AssignedSpeciesName != "" {
			speciesNames[p.AssignedSpeciesName] = struct{}{}
		}
	}

	speciesResult, err := svc.clients.SpeciesService.ListSpecies(ctx, connect.NewRequest(&treatmentv1.ListSpeciesRequest{
		Names: slices.Collect(maps.Keys(speciesNames)),
	}))
	if err != nil {
		return nil, fmt.Errorf("failed to get species data: %w", err)
	}

	return connect.NewResponse(&customerv1.GetPatientsByCustomerResponse{
		Patients: patients,
		Species:  speciesResult.Msg.Species,
	}), nil
}

func (svc *PatientService) QueryPatients(ctx context.Context, req *connect.Request[customerv1.QueryPatientsRequests]) (*connect.Response[customerv1.QueryPatientsResponse], error) {
	patients, err := svc.repository.QueryPatients(ctx, req.Msg.Query)
	if err != nil {
		return nil, err
	}

	speciesNames := make(map[string]struct{})
	for _, p := range patients {
		if p.AssignedSpeciesName != "" {
			speciesNames[p.AssignedSpeciesName] = struct{}{}
		}
	}

	speciesResult, err := svc.clients.SpeciesService.ListSpecies(ctx, connect.NewRequest(&treatmentv1.ListSpeciesRequest{
		Names: slices.Collect(maps.Keys(speciesNames)),
	}))
	if err != nil {
		return nil, fmt.Errorf("failed to get species data: %w", err)
	}

	return connect.NewResponse(&customerv1.QueryPatientsResponse{
		Patients: patients,
		Species:  speciesResult.Msg.Species,
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

func (svc *PatientService) AddAnamnesis(ctx context.Context, req *connect.Request[customerv1.AddAnamnesisRequest]) (*connect.Response[emptypb.Empty], error) {
	var (
		t         time.Time
		patientID string
	)

	switch v := req.Msg.Reference.(type) {
	case *customerv1.AddAnamnesisRequest_AdditionUniqueId:
		p, err := svc.repository.LookupPatientByAdditionalUniqueId(ctx, v.AdditionUniqueId)
		if err != nil {
			return nil, fmt.Errorf("failed to find patient by additional_unique_id: %w", err)
		}
		patientID = p.PatientId

	case *customerv1.AddAnamnesisRequest_PatientId:
		p, err := svc.repository.LookupPatientByAdditionalUniqueId(ctx, v.PatientId)
		if err != nil {
			return nil, fmt.Errorf("failed to find patient by patient_id: %w", err)
		}
		patientID = p.PatientId

	case *customerv1.AddAnamnesisRequest_PatientImportReference:
		p, err := svc.repository.LookupPatientByRef(ctx, v.PatientImportReference.Importer, v.PatientImportReference.InternalReference)
		if err != nil {
			return nil, fmt.Errorf("failed to find patient by patient_import_reference: %w", err)
		}
		patientID = p.PatientId

	default:
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("unsupported patient reference type"))
	}

	if req.Msg.GetAnamnesis().Time.IsValid() {
		t = req.Msg.GetAnamnesis().Time.AsTime()
	}

	if err := svc.repository.AddAnamnesis(ctx, patientID, req.Msg.Anamnesis.Order, t, req.Msg.Anamnesis.Diagnosis, req.Msg.Anamnesis.Text); err != nil {
		return nil, err
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (svc *PatientService) GetAnamnesis(ctx context.Context, req *connect.Request[customerv1.GetAnamnesisRequest]) (*connect.Response[customerv1.GetAnamnesisResponse], error) {
	var (
		from time.Time
		to   time.Time
	)

	if tr := req.Msg.GetTimeRange(); tr != nil {
		if t := tr.From; t.IsValid() {
			from = t.AsTime()
		}

		if t := tr.To; t.IsValid() {
			to = t.AsTime()
		}
	}

	res, err := svc.repository.GetAnamnesis(ctx, req.Msg.PatientId, from, to)
	if err != nil {
		return nil, err
	}

	var patient *customerv1.Patient
	if fm := req.Msg.ReadMask; fm != nil && len(fm.Paths) > 0 {
		for _, p := range fm.Paths {
			if strings.HasPrefix(p, "patient") {
				var err error
				patient, err = svc.repository.LookupPatientById(ctx, req.Msg.PatientId)
				if err != nil {
					return nil, fmt.Errorf("failed to load patient: %w", err)
				}

				break
			}
		}
	}

	response := &customerv1.GetAnamnesisResponse{
		Anamnesis: res,
		Patient:   patient,
	}

	if fm := req.Msg.ReadMask; fm != nil && len(fm.Paths) > 0 {
		fmutils.Prune(response, fm.Paths)
	}

	return connect.NewResponse(response), nil
}
