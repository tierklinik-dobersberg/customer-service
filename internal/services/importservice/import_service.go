package importservice

import (
	"context"

	"github.com/bufbuild/connect-go"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"github.com/tierklinik-dobersberg/customer-service/internal/session"
)

type ImportService struct {
	repo     repo.Repo
	resolver session.PriorityResolver

	customerv1connect.UnimplementedCustomerImportServiceHandler
}

func NewImportService(repo repo.Repo, resolver session.PriorityResolver) *ImportService {
	return &ImportService{
		repo:     repo,
		resolver: resolver,
	}
}

func (svc *ImportService) ImportSession(ctx context.Context, stream *connect.BidiStream[customerv1.ImportSessionRequest, customerv1.ImportSessionResponse]) error {
	// create a new import session hand start handling customer updates.
	session := session.NewImportSession(stream, svc.repo, svc.resolver)

	return session.Handle(ctx)
}
