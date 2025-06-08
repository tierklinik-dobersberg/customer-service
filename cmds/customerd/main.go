package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/bufbuild/connect-go"
	"github.com/sirupsen/logrus"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/discovery/wellknown"
	"github.com/tierklinik-dobersberg/apis/pkg/service"
	"github.com/tierklinik-dobersberg/customer-service/internal/config"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo/mongo"
	"github.com/tierklinik-dobersberg/customer-service/internal/services/customerservice"
	"github.com/tierklinik-dobersberg/customer-service/internal/services/importservice"
	"github.com/tierklinik-dobersberg/customer-service/internal/services/patient"
)

type resolver map[string]int

func (r resolver) IsAllowed(importer string, owners []string) bool {
	p := r[importer]

	for _, o := range owners {
		if r[o] > p {
			return false
		}
	}

	return true
}

var serverContextKey = struct{ S string }{S: "serverContextKey"}

func main() {
	ctx := context.Background()

	instance, err := service.Configure(
		wellknown.CustomerV1ServiceScope,
		config.Config{},
	)
	if err != nil {
		slog.Error("failed to configure service instance", "error", err)
		os.Exit(1)
	}

	backend, err := mongo.New(ctx, instance.Database)
	if err != nil {
		logrus.Fatalf("failed to create repository: %s", err)
	}

	repository := repo.New(backend, backend)

	resolver := resolver{
		"user":    2,
		"vetinf":  1,
		"carddav": 0,
	}

	cfg := &instance.Config

	// create a new CallService and add it to the mux.
	importService := importservice.NewImportService(cfg, repository, repository, resolver, instance.Catalog)
	customerService := customerservice.New(cfg, repository, resolver)
	patientService := patient.New(cfg, repository)

	options := connect.WithOptions(instance.ConnectOptions()...)

	path, handler := customerv1connect.NewCustomerImportServiceHandler(importService, options)
	instance.Mux.Shared.Handle(path, handler)

	path, handler = customerv1connect.NewCustomerServiceHandler(customerService, options)
	instance.Mux.Shared.Handle(path, handler)
	instance.Mux.Shared.Handle("/crm/lookup", http.HandlerFunc(customerService.CRMLookupHandler))

	path, handler = customerv1connect.NewPatientServiceHandler(patientService, options)
	instance.Mux.Shared.Handle(path, handler)

	if err := instance.Run(); err != nil {
		slog.Error("failed to serve", "error", err)
		os.Exit(1)
	}
}
