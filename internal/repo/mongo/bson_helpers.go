package mongo

import (
	"bytes"
	"encoding/json"
	"fmt"

	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/encoding/protojson"
)

func (repo *Repository) bsonToCustomer(document bson.M) (*customerv1.CustomerResponse, error) {
	json, err := bson.MarshalExtJSON(document, true, false)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal BSON as JSON: %w", err)
	}

	unmarshaler := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}

	var customer = new(customerv1.CustomerResponse)
	if err := unmarshaler.Unmarshal(json, customer); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to protobuf message: %w", err)
	}

	switch v := document["_id"].(type) {
	case string:
		customer.Customer.Id = v
	case primitive.ObjectID:
		customer.Customer.Id = v.Hex()

	default:
		return customer, fmt.Errorf("invalid or unsupported document _id type: %T", v)
	}

	return customer, nil
}

func (repo *Repository) customerToBSON(customer *customerv1.CustomerResponse) (bson.M, error) {
	opts := protojson.MarshalOptions{
		Multiline: true,
		Indent:    "  ",
	}

	blob, err := opts.Marshal(customer)
	if err != nil {
		return nil, fmt.Errorf("failed to convert proto.Message to JSON: %w", err)
	}

	vr, err := bsonrw.NewExtJSONValueReader(bytes.NewReader(blob), true)
	if err != nil {
		return nil, fmt.Errorf("failed to create ext. JSON reader: %w", err)
	}
	dec, err := bson.NewDecoder(vr)
	if err != nil {
		return nil, fmt.Errorf("failed to create BSON decoder: %w", err)
	}
	dec.DefaultDocumentM()

	var m bson.M
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to decode extended JSON to BSON: %w", err)
	}

	if customer.Customer.Id != "" {
		var err error

		m["_id"], err = primitive.ObjectIDFromHex(customer.Customer.Id)
		if err != nil {
			return nil, fmt.Errorf("failed to parse document id: %w", err)
		}
	}

	return m, nil
}

func (repo *Repository) bsonToPatient(document bson.M) (*customerv1.Patient, error) {
	jsonBlob, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal BSON as JSON: %w", err)
	}

	unmarshaler := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}

	var patient = new(customerv1.Patient)
	if err := unmarshaler.Unmarshal(jsonBlob, patient); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to protobuf message: %w (blob: %s)", err, string(jsonBlob))
	}

	switch v := document["_id"].(type) {
	case string:
		patient.PatientId = v
	case primitive.ObjectID:
		patient.PatientId = v.Hex()

	default:
		return patient, fmt.Errorf("invalid or unsupported document _id type: %T", v)
	}

	return patient, nil
}

func (repo *Repository) patientToBSON(patient *customerv1.Patient) (bson.M, error) {
	opts := protojson.MarshalOptions{
		Multiline: true,
		Indent:    "  ",
	}

	blob, err := opts.Marshal(patient)
	if err != nil {
		return nil, fmt.Errorf("failed to convert proto.Message to JSON: %w", err)
	}

	vr, err := bsonrw.NewExtJSONValueReader(bytes.NewReader(blob), true)
	if err != nil {
		return nil, fmt.Errorf("failed to create ext. JSON reader: %w", err)
	}
	dec, err := bson.NewDecoder(vr)
	if err != nil {
		return nil, fmt.Errorf("failed to create BSON decoder: %w", err)
	}
	dec.DefaultDocumentM()

	var m bson.M
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to decode extended JSON to BSON: %w", err)
	}

	if patient.PatientId != "" {
		var err error

		m["_id"], err = primitive.ObjectIDFromHex(patient.PatientId)
		if err != nil {
			return nil, fmt.Errorf("failed to parse document id: %w", err)
		}
	}

	return m, nil
}
