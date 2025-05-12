package mongo

import (
	"bytes"
	"fmt"

	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/encoding/protojson"
)

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
