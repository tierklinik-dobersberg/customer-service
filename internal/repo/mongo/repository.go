package mongo

import (
	"context"
	"fmt"

	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Repository struct {
	customers *mongo.Collection
	patients  *mongo.Collection
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
		patients:  db.Collection("patients"),
		locks:     db.Collection("locks"),
	}

	if err := repo.setup(ctx); err != nil {
		return nil, fmt.Errorf("failed to setup collection: %w", err)
	}

	return repo, nil
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

	if _, err := repo.patients.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{
					Key:   "customerId",
					Value: 1,
				},
				{
					Key:   "patientName",
					Value: 1,
				},
			},
		},
		{
			Keys: bson.D{
				{
					Key:   "importer",
					Value: 1,
				},
				{
					Key:   "internalReference",
					Value: 1,
				},
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to create patient indices: %w", err)
	}

	return nil
}

// Compile-time check
var _ repo.CustomerBackend = (*Repository)(nil)
var _ repo.MultiCustomerQueryRunner = (*Repository)(nil)
var _ repo.PatientBackend = (*Repository)(nil)
