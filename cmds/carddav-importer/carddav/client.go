package carddav

import (
	"context"
	"fmt"
	"net/http"

	"github.com/emersion/go-webdav"
	"github.com/emersion/go-webdav/carddav"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Client supports basic CardDAV operations.
type Client struct {
	cli *carddav.Client
	cfg *CardDAVConfig
}

// NewClient returns a new CardDAV client.
func NewClient(ctx context.Context, cfg *CardDAVConfig) (*Client, error) {
	var cli webdav.HTTPClient = http.DefaultClient

	if cfg.User != "" {
		cli = webdav.HTTPClientWithBasicAuth(cli, cfg.User, cfg.Password)
	}

	davcli, err := carddav.NewClient(cli, cfg.Server)
	if err != nil {
		return nil, err
	}

	if err := davcli.HasSupport(ctx); err != nil {
		return nil, err
	}

	return &Client{
		cfg: cfg,
		cli: davcli,
	}, nil
}

func (cli *Client) Sync(ctx context.Context, col, syncToken string) (<-chan string, <-chan *carddav.AddressObject, string, error) {
	syncResponse, err := cli.cli.SyncCollection(ctx, col, &carddav.SyncQuery{
		SyncToken: syncToken,
	})
	if err != nil {
		return nil, nil, "", err
	}

	deleted := make(chan string, 100)
	updated := make(chan *carddav.AddressObject, 100)

	logrus.Infof("carddav: received sync response with %d deletes and %d updates", len(syncResponse.Deleted), len(syncResponse.Updated))

	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		for _, d := range syncResponse.Deleted {
			select {
			case deleted <- d:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})

	wg.Go(func() error {
		// either emersion/webdav does not correctly handle AllProps: true in the sync-query
		// or radicale is ingoring it. Nonetheless, we need to fetch all address objects in
		// batches using MultiGet.
		batchSize := 20
		for i := 0; i < len(syncResponse.Updated); i += batchSize {
			paths := make([]string, 0, batchSize)
			for j := 0; j < batchSize && (i+j) < len(syncResponse.Updated); j++ {
				paths = append(paths, syncResponse.Updated[i+j].Path)
			}

			objs, err := cli.cli.MultiGetAddressBook(ctx, col, &carddav.AddressBookMultiGet{
				Paths: paths,
				DataRequest: carddav.AddressDataRequest{
					AllProp: true,
				},
			})
			if err != nil {
				return fmt.Errorf("failed to retrieve batch: %w", err)
			}
			for idx := range objs {
				select {
				case updated <- &objs[idx]:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		return nil
	})

	go func() {
		defer close(deleted)
		defer close(updated)
		if err := wg.Wait(); err != nil {
			logrus.Errorf(err.Error())
		}
	}()

	return deleted, updated, syncResponse.SyncToken, nil
}

func (cli *Client) DeleteObject(ctx context.Context, path string) error {
	if err := cli.cli.RemoveAll(ctx, path); err != nil {
		return err
	}

	return nil
}

func (cli *Client) ListAddressBooks(ctx context.Context) ([]carddav.AddressBook, error) {
	if err := cli.cli.HasSupport(ctx); err != nil {
		return nil, err
	}

	up, err := cli.cli.FindCurrentUserPrincipal(ctx)
	if err != nil {
		return nil, err
	}

	hs, err := cli.cli.FindAddressBookHomeSet(ctx, up)
	if err != nil {
		return nil, err
	}

	books, err := cli.cli.FindAddressBooks(ctx, hs)
	if err != nil {
		return nil, err
	}

	return books, nil
}
