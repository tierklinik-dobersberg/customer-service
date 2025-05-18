package mongo

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hashicorp/go-multierror"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/ql/bsonql"
	"github.com/tierklinik-dobersberg/apis/pkg/timeutil"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/proto"
)

func (r *Repository) StorePatient(ctx context.Context, p *customerv1.Patient) (*customerv1.Patient, error) {
	document, err := models.PatientFromProto(p)
	if err != nil {
		return nil, fmt.Errorf("invalid patient: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to prepare BSON document: %w", err)
	}

	res := proto.Clone(p).(*customerv1.Patient)

	if id := p.PatientId; id != "" {
		res, err := r.patients.ReplaceOne(ctx, bson.M{"_id": document.PatientId}, document)
		if err != nil {
			return nil, fmt.Errorf("failed to replace patient %q: %w", id, err)
		}

		if res.MatchedCount == 0 {
			return nil, fmt.Errorf("failed to replace patient %q: %w", id, repo.ErrNotFound)
		}

	} else {
		document.PatientId = primitive.NewObjectID()

		_, err := r.patients.InsertOne(ctx, document)
		if err != nil {
			return nil, fmt.Errorf("failed to insert patient: %w", err)
		}

		res.PatientId = document.PatientId.Hex()
	}

	return res, nil
}

func (r *Repository) QueryPatients(ctx context.Context, query string) ([]*customerv1.Patient, error) {
	p := &bsonql.BSONQL{
		Schema: models.PatientSchema,
	}

	filter, err := p.Parse(query)
	if err != nil {
		return nil, err
	}

	res, err := r.patients.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to search for patients: %w", err)
	}

	var result []models.Patient
	if err := res.All(ctx, &result); err != nil {
		return nil, fmt.Errorf("failed to decode patient records: %w", err)
	}

	var merr = new(multierror.Error)
	pbResult := make([]*customerv1.Patient, 0, len(result))
	for _, p := range result {
		pb, err := p.ToProto()
		if err != nil {
			merr.Errors = append(merr.Errors, err)
			continue
		}

		pbResult = append(pbResult, pb)
	}

	return pbResult, merr.ErrorOrNil()
}

func (r *Repository) LookupPatientsByCustomerId(ctx context.Context, id string) ([]*customerv1.Patient, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, fmt.Errorf("invalid customer id")
	}

	res, err := r.patients.Find(ctx, bson.M{
		"customerId": oid,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search for patients: %w", err)
	}

	var result []models.Patient
	if err := res.All(ctx, &result); err != nil {
		return nil, fmt.Errorf("failed to decode patient records: %w", err)
	}

	var merr = new(multierror.Error)
	pbResult := make([]*customerv1.Patient, 0, len(result))
	for _, p := range result {
		pb, err := p.ToProto()
		if err != nil {
			merr.Errors = append(merr.Errors, err)
			continue
		}

		pbResult = append(pbResult, pb)
	}

	return pbResult, merr.ErrorOrNil()
}

func (r *Repository) LookupPatientById(ctx context.Context, id string) (*customerv1.Patient, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}

	res := r.patients.FindOne(ctx, bson.M{"_id": oid})
	if res.Err() != nil {
		return nil, convertErr(res.Err())
	}

	var m models.Patient
	if err := res.Decode(&m); err != nil {
		return nil, err
	}

	return m.ToProto()
}

func (r *Repository) LookupPatientByAdditionalUniqueId(ctx context.Context, id string) (*customerv1.Patient, error) {
	res := r.patients.FindOne(ctx, bson.M{"additionalUniqueId": id})
	if res.Err() != nil {
		return nil, convertErr(res.Err())
	}

	var m models.Patient
	if err := res.Decode(&m); err != nil {
		return nil, err
	}

	return m.ToProto()
}

func (r *Repository) LookupPatientByRef(ctx context.Context, importer, ref string) (*customerv1.Patient, error) {
	filter := bson.M{
		"importer":          importer,
		"internalReference": ref,
	}

	slog.DebugContext(ctx, "searching patients by internal reference", slog.Any("filter", filter))

	res := r.patients.FindOne(ctx, filter)
	if res.Err() != nil {
		return nil, convertErr(res.Err())
	}

	var m models.Patient
	if err := res.Decode(&m); err != nil {
		return nil, err
	}

	return m.ToProto()
}

func (r *Repository) LockPatient(ctx context.Context, id string) (func(), error) {
	_, err := r.locks.InsertOne(ctx, bson.M{
		"id":       id,
		"lockedAt": time.Now(),
	})

	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return func() {}, repo.ErrRecordLocked
		}

		return func() {}, fmt.Errorf("failed to create customer lock: %w", err)
	}

	return func() {
		res := r.locks.FindOneAndDelete(context.Background(), bson.M{
			"id": id,
		})

		if res.Err() != nil {
			slog.Error("failed to unlock record", slog.Attr{
				Key:   "error",
				Value: slog.StringValue(res.Err().Error()),
			})
		}
	}, nil
}

func (r *Repository) GetAnamnesis(ctx context.Context, patientId string, from, to time.Time) ([]*customerv1.Anamnesis, error) {
	pid, err := primitive.ObjectIDFromHex(patientId)
	if err != nil {
		return nil, fmt.Errorf("invalid object ID: %w", err)
	}

	filter := bson.M{
		"patientId": pid,
	}

	switch {
	case !from.IsZero() && !to.IsZero():
		filter["createdAt"] = bson.M{
			"$gte": timeutil.StartOfDay(from),
			"$lte": timeutil.EndOfDay(to),
		}

	case !from.IsZero():
		filter["createdAt"] = bson.M{
			"$gte": timeutil.StartOfDay(from),
		}

	case !to.IsZero():
		filter["createdAt"] = bson.M{
			"$lte": timeutil.EndOfDay(to),
		}
	}

	result, err := r.anamnesis.Find(ctx, filter, options.Find().SetSort(bson.D{
		{
			Key:   "order",
			Value: 1,
		},
	}))

	if err != nil {
		return nil, fmt.Errorf("failed to perform find operation: %w", err)
	}

	var data []models.Anamnesis
	if err := result.All(ctx, &data); err != nil {
		return nil, fmt.Errorf("failed to decode documents: %w", err)
	}

	pbResult := make([]*customerv1.Anamnesis, 0, len(data))
	for _, a := range data {
		pbResult = append(pbResult, a.ToProto())
	}

	return pbResult, nil
}

func (r *Repository) AddAnamnesis(ctx context.Context, patientId string, order int64, t time.Time, diagnosis, text string) error {
	pid, err := primitive.ObjectIDFromHex(patientId)
	if err != nil {
		return fmt.Errorf("invalid object ID: %w", err)
	}

	a := models.Anamnesis{
		PatientID: pid,
		Diagnosis: diagnosis,
		Text:      text,
		CreatedAt: t,
		Order:     order,
	}

	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now()
	}

	opts := options.Replace().SetUpsert(true)
	_, err = r.anamnesis.ReplaceOne(ctx, bson.M{"order": a.Order}, opts)

	if err != nil {
		return fmt.Errorf("failed to persist record: %w", err)
	}

	return nil
}
