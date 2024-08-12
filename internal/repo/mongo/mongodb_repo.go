package mongo

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hashicorp/go-multierror"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/encoding/protojson"
)

type Repository struct {
	customers *mongo.Collection
	locks     *mongo.Collection
}

func New(ctx context.Context, uri, dbName string) (*Repository, error) {
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("failed to create mongodb client: %w", err)
	}

	if err := cli.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping mongodb server: %w", err)
	}

	db := cli.Database(dbName)

	repo := &Repository{
		customers: db.Collection("customers"),
		locks:     db.Collection("locks"),
	}

	if err := repo.setup(ctx); err != nil {
		return nil, fmt.Errorf("failed to setup collection: %w", err)
	}

	return repo, nil
}

func (repo *Repository) StoreCustomer(ctx context.Context, customer *customerv1.Customer, states []*customerv1.ImportState) error {
	document, err := repo.customerToBSON(&customerv1.CustomerResponse{
		Customer: customer,
		States:   states,
	})

	if err != nil {
		return fmt.Errorf("failed to prepare BSON document: %w", err)
	}

	res, err := repo.customers.InsertOne(ctx, document)
	if err != nil {
		return fmt.Errorf("failed to insert customer: %w", err)
	}

	// make sure the new customer ID is set on the protobuf message
	customer.Id = res.InsertedID.(primitive.ObjectID).Hex()

	return nil
}

func (r *Repository) LockCustomer(ctx context.Context, id string) (func(), error) {
	_, err := r.locks.InsertOne(ctx, bson.M{
		"id":       id,
		"lockedAt": time.Now(),
	})

	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return func() {}, repo.ErrCustomerLocked
		}

		return func() {}, fmt.Errorf("failed to create customer lock: %w", err)
	}

	return func() {
		res := r.locks.FindOneAndDelete(context.Background(), bson.M{
			"id": id,
		})

		if res.Err() != nil {
			slog.Error("failed to unlock customer", slog.Attr{
				Key:   "error",
				Value: slog.StringValue(res.Err().Error()),
			})
		}
	}, nil
}

func (r *Repository) LookupCustomerById(ctx context.Context, id string) (*customerv1.Customer, []*customerv1.ImportState, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, nil, err
	}

	res := r.customers.FindOne(ctx, bson.M{"_id": oid})
	if res.Err() != nil {
		return nil, nil, res.Err()
	}

	var m bson.M
	if err := res.Decode(&m); err != nil {
		return nil, nil, err
	}

	customer, err := r.bsonToCustomer(m)
	if err != nil {
		return nil, nil, err
	}

	return customer.Customer, customer.States, nil
}

func (r *Repository) LookupCustomerByRef(ctx context.Context, importer, ref string) (*customerv1.Customer, []*customerv1.ImportState, error) {
	filter := bson.M{
		"states": bson.M{
			"$elemMatch": bson.M{
				"importer":          importer,
				"internalReference": ref,
			},
		},
	}

	res := r.customers.FindOne(ctx, filter)
	if res.Err() != nil {
		return nil, nil, res.Err()
	}

	var m bson.M
	if err := res.Decode(&m); err != nil {
		return nil, nil, err
	}

	customer, err := r.bsonToCustomer(m)
	if err != nil {
		return nil, nil, err
	}

	return customer.Customer, customer.States, nil
}

func (r *Repository) LookupCustomerByName(ctx context.Context, name string) ([]*customerv1.CustomerResponse, error) {
	return r.searchCustomers(ctx, bson.M{
		"customer.lastName": bson.M{
			"$text": bson.M{
				"$search": name,
			},
		},
	})
}

func (r *Repository) LookupCustomerByMail(ctx context.Context, mail string) ([]*customerv1.CustomerResponse, error) {
	return r.searchCustomers(ctx, bson.M{
		"customer.emailAddresses": bson.M{
			"$elemMatch": mail,
		},
	})
}

func (r *Repository) LookupCustomerByPhone(ctx context.Context, phone string) ([]*customerv1.CustomerResponse, error) {
	return r.searchCustomers(ctx, bson.M{
		"customer.phoneNumbers": bson.M{
			"$elemMatch": phone,
		},
	})
}

func (r *Repository) searchCustomers(ctx context.Context, filter bson.M) ([]*customerv1.CustomerResponse, error) {
	res, err := r.customers.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to perform find operation: %w", err)
	}

	var (
		results []*customerv1.CustomerResponse
		merr    = new(multierror.Error)
	)

	for res.Next(ctx) {
		var m bson.M
		if err := res.Decode(&m); err != nil {
			merr.Errors = append(merr.Errors, fmt.Errorf("failed to decode record: %w", err))
			continue
		}

		customer, err := r.bsonToCustomer(m)
		if err != nil {
			merr.Errors = append(merr.Errors, fmt.Errorf("failed to convert record from BSON: %w", err))
			continue
		}

		results = append(results, customer)
	}

	return results, nil

}

func (repo *Repository) setup(ctx context.Context) error {
	if _, err := repo.locks.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "id", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return err
	}

	if _, err := repo.customers.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{
					Key:   "customer.lastName",
					Value: "text",
				},
			},
			Options: options.Index().SetSparse(true),
		},
		{
			Keys: bson.D{
				{
					Key:   "customer.emailAddresses",
					Value: 1,
				},
			},
			Options: options.Index().SetSparse(true),
		},
		{
			Keys: bson.D{
				{
					Key:   "customer.phoneNumbers",
					Value: 1,
				},
			},
			Options: options.Index().SetSparse(true),
		},
		{
			Keys: bson.D{
				{
					Key:   "states.importer",
					Value: 1,
				},
				{
					Key:   "states.internalReference",
					Value: 1,
				},
			},
			Options: options.Index().SetUnique(true).SetSparse(true),
		},
	}); err != nil {
		return fmt.Errorf("failed to create customer indices: %w", err)
	}

	return nil
}

func (repo *Repository) bsonToCustomer(document bson.M) (*customerv1.CustomerResponse, error) {
	json, err := bson.MarshalExtJSON(document, true, false)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal BSON as JSON: %w", err)
	}

	var customer = new(customerv1.CustomerResponse)
	if err := protojson.Unmarshal(json, customer); err != nil {
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

// Compile-time check
var _ repo.Backend = (*Repository)(nil)
