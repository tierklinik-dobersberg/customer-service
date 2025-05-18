package repo

import (
	"context"
	"time"

	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
)

type PatientBackend interface {
	StorePatient(context.Context, *customerv1.Patient) (*customerv1.Patient, error)
	LookupPatientByRef(context.Context, string, string) (*customerv1.Patient, error)
	LookupPatientById(context.Context, string) (*customerv1.Patient, error)
	LookupPatientByAdditionalUniqueId(context.Context, string) (*customerv1.Patient, error)
	LookupPatientsByCustomerId(context.Context, string) ([]*customerv1.Patient, error)
	QueryPatients(context.Context, string) ([]*customerv1.Patient, error)
	LockPatient(ctx context.Context, id string) (func(), error)

	AddAnamnesis(ctx context.Context, patientId, reference string, t time.Time, diagnosis, text string) error
	GetAnamnesis(ctx context.Context, patientId string, from, to time.Time) ([]*customerv1.Anamnesis, error)
}
