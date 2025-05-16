package repo

import (
	"context"

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
}
