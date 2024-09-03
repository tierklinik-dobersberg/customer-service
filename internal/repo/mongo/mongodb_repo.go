package mongo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/nyaruka/phonenumbers"
	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
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

func (r *Repository) StoreCustomer(ctx context.Context, customer *customerv1.Customer, states []*customerv1.ImportState) error {
	document, err := r.customerToBSON(&customerv1.CustomerResponse{
		Customer: customer,
		States:   states,
	})

	if err != nil {
		return fmt.Errorf("failed to prepare BSON document: %w", err)
	}

	if customer.Id != "" {
		oid, err := primitive.ObjectIDFromHex(customer.Id)
		if err != nil {
			return fmt.Errorf("invalid customer id %q: %w", customer.Id, err)
		}

		res, err := r.customers.ReplaceOne(ctx, bson.M{"_id": oid}, document)
		if err != nil {
			return fmt.Errorf("failed to replace customer %q: %w", customer.Id, err)
		}

		if res.MatchedCount == 0 {
			return fmt.Errorf("failed to replace customer %q: %w", customer.Id, repo.ErrCustomerNotFound)
		}

	} else {
		res, err := r.customers.InsertOne(ctx, document)
		if err != nil {
			return fmt.Errorf("failed to insert customer: %w", err)
		}

		customer.Id = res.InsertedID.(primitive.ObjectID).Hex()
	}

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

func (r *Repository) ListCustomers(ctx context.Context, p *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error) {
	return r.searchCustomers(ctx, bson.M{}, p)
}

func (r *Repository) LookupCustomerById(ctx context.Context, id string) (*customerv1.Customer, []*customerv1.ImportState, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, nil, err
	}

	res := r.customers.FindOne(ctx, bson.M{"_id": oid})
	if res.Err() != nil {
		return nil, nil, convertErr(res.Err())
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

	slog.DebugContext(ctx, "searching customer by internal reference", slog.Any("filter", filter))

	res := r.customers.FindOne(ctx, filter)
	if res.Err() != nil {
		return nil, nil, convertErr(res.Err())
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

func (r *Repository) LookupCustomerByName(ctx context.Context, name string, p *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error) {
	slog.InfoContext(ctx, "searching customers by name", slog.Any("name", name))

	return r.searchCustomers(ctx, bson.M{
		"$text": bson.M{
			"$search": name,
		},
	}, p)
}

func (r *Repository) LookupCustomerByMail(ctx context.Context, mail string, p *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error) {
	slog.InfoContext(ctx, "searching customers by mail", slog.Any("mail", mail))

	return r.searchCustomers(ctx, bson.M{
		"customer.emailAddresses": mail,
	}, p)
}

func (r *Repository) LookupCustomerByPhone(ctx context.Context, phone string, p *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error) {
	slog.InfoContext(ctx, "searching customers by phone number", slog.Any("phstringone", phone))

	return r.searchCustomers(ctx, bson.M{
		"customer.phoneNumbers": phone,
	}, p)
}

func (r *Repository) SearchQueries(ctx context.Context, queries []*customerv1.CustomerQuery, p *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error) {
	var phoneNumbers []string
	var lastNames string
	var ids []primitive.ObjectID
	var mails []string

	for _, q := range queries {
		switch v := q.Query.(type) {
		case *customerv1.CustomerQuery_EmailAddress:
			mails = append(mails, v.EmailAddress)
		case *customerv1.CustomerQuery_PhoneNumber:
			phoneNumbers = append(phoneNumbers, v.PhoneNumber)
		case *customerv1.CustomerQuery_Id:
			oid, err := primitive.ObjectIDFromHex(v.Id)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to convert customer id to object id: %w", err)
			}

			ids = append(ids, oid)
		case *customerv1.CustomerQuery_Name:
			if v.Name.LastName != "" {
				lastNames = fmt.Sprintf("%s %q", lastNames, v.Name.LastName)
			}
		case *customerv1.CustomerQuery_InternalReference:
			return nil, 0, fmt.Errorf("internal reference is not supported in SearchQueries yet")
		}
	}

	ors := bson.A{}

	if len(phoneNumbers) > 0 {
		var formatted []string

		for _, p := range phoneNumbers {
			parsed, err := phonenumbers.Parse(p, "AT")
			if err == nil {
				formatted = append(formatted, phonenumbers.Format(parsed, phonenumbers.INTERNATIONAL))
			} else {
				formatted = append(formatted, p)
			}
		}

		ors = append(ors, bson.E{
			Key: "customer.phoneNumbers",
			Value: bson.M{
				"$in": formatted,
			},
		})
	}

	if len(mails) > 0 {
		ors = append(ors, bson.E{
			Key: "customer.emailAddresses",
			Value: bson.M{
				"$in": mails,
			},
		})
	}

	if len(ids) > 0 {
		ors = append(ors, bson.E{
			Key: "_id",
			Value: bson.M{
				"$in": ids,
			},
		})
	}

	filter := bson.M{}

	switch len(ors) {
	case 0:
	case 1:
		filter[ors[0].(bson.E).Key] = ors[0].(bson.E).Value
	default:
		filter["$or"] = ors
	}

	if len(lastNames) > 0 {
		filter["$text"] = bson.M{
			"$search": lastNames,
		}
	}

	return r.searchCustomers(ctx, filter, p)
}

func (r *Repository) searchCustomers(ctx context.Context, filters bson.M, p *commonv1.Pagination) ([]*customerv1.CustomerResponse, int, error) {
	slog.DebugContext(ctx, "searching customers", slog.Any("filter", filters))

	pagination := []bson.D{}

	if p != nil {
		if len(p.SortBy) > 0 {
			sort := bson.D{}
			for _, field := range p.SortBy {
				var dir int
				switch field.Direction {
				case commonv1.SortDirection_SORT_DIRECTION_ASC:
					dir = 1
				default:
					dir = -1
				}

				sort = append(sort, bson.E{Key: field.FieldName, Value: dir})
			}

			pagination = append(pagination, bson.D{
				{Key: "$sort", Value: sort},
			})
		}

		if p.PageSize > 0 {
			pagination = append(pagination, bson.D{{Key: "$skip", Value: p.PageSize * p.GetPage()}})
			pagination = append(pagination, bson.D{{Key: "$limit", Value: p.PageSize}})
		}
	}

	aggregation := mongo.Pipeline{
		bson.D{
			{
				Key:   "$match",
				Value: filters,
			},
		},
		bson.D{
			{
				Key: "$facet",
				Value: bson.M{
					"metadata": []bson.D{
						{
							{Key: "$count", Value: "totalCount"},
						},
					},
					"data": pagination,
				},
			},
		},
	}

	blob, _ := json.MarshalIndent(aggregation, "", "  ")
	log.Println(string(blob))

	res, err := r.customers.Aggregate(ctx, aggregation)
	if err != nil {
		slog.Error("failed to perform aggregate", "error", err)
		return nil, 0, err
	}

	/*
		var tmp []any
		if err := res.All(ctx, &tmp); err != nil {
			return nil, 0, err
		}

		blob, _ = json.MarshalIndent(tmp, "", "  ")
		log.Println(string(blob))
	*/

	var result []struct {
		Metadata []struct {
			TotalCount int `bson:"totalCount"`
		} `bson:"metadata"`
		Data []bson.M
	}

	if err := res.All(ctx, &result); err != nil {
		return nil, 0, fmt.Errorf("failed to decode result: %w", err)
	}

	// nothing found
	if len(result) == 0 {
		return nil, 0, nil
	}

	if len(result) > 1 {
		slog.Warn("received unexpected result count for aggregation state", "count", len(result))
	}

	var (
		results []*customerv1.CustomerResponse
		merr    = new(multierror.Error)
	)

	for _, m := range result[0].Data {
		customer, err := r.bsonToCustomer(m)
		if err != nil {
			merr.Errors = append(merr.Errors, fmt.Errorf("failed to convert record from BSON: %w", err))

			continue
		}

		results = append(results, customer)
	}

	if res.Err() != nil {
		merr.Errors = append(merr.Errors, fmt.Errorf("mongodb cursor error: %w", res.Err()))
	}

	if len(result[0].Metadata) == 0 {
		slog.Warn("got empty metadata response")
		return results, len(results), merr.ErrorOrNil()
	}

	return results, result[0].Metadata[0].TotalCount, merr.ErrorOrNil()

}

func (repo *Repository) setup(ctx context.Context) error {
	repo.customers.Indexes().DropOne(ctx, "customer.lastName_text")

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
				{
					Key:   "customer.firstName",
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

// Compile-time check
var _ repo.Backend = (*Repository)(nil)

func convertErr(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, mongo.ErrNoDocuments) {
		return repo.ErrCustomerNotFound
	}

	return err
}
