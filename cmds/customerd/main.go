package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/bufbuild/connect-go"
	"github.com/bufbuild/protovalidate-go"
	"github.com/sirupsen/logrus"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1/idmv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/apis/pkg/cors"
	"github.com/tierklinik-dobersberg/apis/pkg/log"
	"github.com/tierklinik-dobersberg/apis/pkg/server"
	"github.com/tierklinik-dobersberg/apis/pkg/validator"
	"github.com/tierklinik-dobersberg/customer-service/internal/config"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo/inmem"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo/mongo"
	"github.com/tierklinik-dobersberg/customer-service/internal/services/customerservice"
	"github.com/tierklinik-dobersberg/customer-service/internal/services/importservice"
	"google.golang.org/protobuf/reflect/protoregistry"
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

func main() {
	ctx := context.Background()

	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		logrus.Fatalf("failed to load config: %s", err)
	}

	roleServiceClient := idmv1connect.NewRoleServiceClient(http.DefaultClient, cfg.IdmURL)

	protoValidator, err := protovalidate.New()
	if err != nil {
		logrus.Fatalf("failed to prepare protovalidator: %s", err)
	}

	authInterceptor := auth.NewAuthAnnotationInterceptor(
		protoregistry.GlobalFiles,
		auth.NewIDMRoleResolver(roleServiceClient),
		auth.RemoteHeaderExtractor)

	interceptors := []connect.Interceptor{
		log.NewLoggingInterceptor(),
		validator.NewInterceptor(protoValidator),
	}

	slog.SetLogLoggerLevel(slog.LevelDebug)

	if os.Getenv("DEBUG") == "" {
		interceptors = append(interceptors, authInterceptor)
	}

	corsConfig := cors.Config{
		AllowedOrigins:   cfg.AllowedOrigins,
		AllowCredentials: true,
	}

	// Prepare our servemux and add handlers.
	serveMux := http.NewServeMux()

	var backend repo.Backend

	if cfg.MongoDBURL != "" {
		var err error
		backend, err = mongo.New(ctx, cfg.MongoDBURL, cfg.MongoDatabaseName)

		if err != nil {
			logrus.Fatalf("failed to create repository: %s", err)
		}
	} else {
		logrus.Warn("using in-memory database, data will not be persisted accross restarts")

		backend = inmem.New()
	}

	store := repo.New(backend)

	resolver := resolver{
		"user":    2,
		"vetinf":  1,
		"carddav": 0,
	}

	// create a new CallService and add it to the mux.
	importService := importservice.NewImportService(store, resolver)
	customerService := customerservice.New(store, resolver)

	path, handler := customerv1connect.NewCustomerImportServiceHandler(importService, connect.WithInterceptors(interceptors...))
	serveMux.Handle(path, handler)

	path, handler = customerv1connect.NewCustomerServiceHandler(customerService, connect.WithInterceptors(interceptors...))
	serveMux.Handle(path, handler)

	loggingHandler := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logrus.Infof("received request: %s %s %s%s", r.Proto, r.Method, r.Host, r.URL.String())

			next.ServeHTTP(w, r)
		})
	}

	// Create the server
	srv, err := server.CreateWithOptions(cfg.ListenAddress, loggingHandler(serveMux), server.WithCORS(corsConfig))
	if err != nil {
		logrus.Fatalf("failed to setup server: %s", err)
	}

	logrus.Infof("HTTP/2 server (h2c) prepared successfully, startin to listen ...")

	if err := server.Serve(ctx, srv); err != nil {
		logrus.Fatalf("failed to serve: %s", err)
	}
}
