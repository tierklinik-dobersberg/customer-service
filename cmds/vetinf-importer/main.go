package main

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	"github.com/tierklinik-dobersberg/customer-service/pkg/importer"
	"golang.org/x/net/http2"
)

var (
	encoding        string
	customerService string
)

func main() {
	if err := getRootCommand().Execute(); err != nil {
		logrus.Fatal(err)
	}
}

func getRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "vetinf-importer [path/to/infdate]",
		Args: cobra.ExactArgs(1),
		Run:  execute,
	}

	f := cmd.Flags()
	{
		f.StringVar(&encoding, "encoding", "IBM852", "The encoding of the VetInf database")
		f.StringVar(&customerService, "server", "https://customers.dobersberg.vet", "The address of the customer service")
	}

	cmd.MarkFlagRequired("server")

	return cmd
}

func execute(cmd *cobra.Command, args []string) {
	exporter, err := NewExporter(args[0], encoding, "AT")
	if err != nil {
		logrus.Fatalf("failed to create vetinf exporter: %s", err)
	}

	transport := &http.Transport{}

	if err := http2.ConfigureTransport(transport); err != nil {
		logrus.Fatalf("failed to configure http/2 transport: %s", err)
	}

	httpCli := newInsecureClient()

	logrus.Infof("http/2 client configured successfully")

	cli := customerv1connect.NewCustomerImportServiceClient(httpCli, customerService)

	stream, _, err := exporter.ExportCustomers(context.Background())
	if err != nil {
		logrus.Fatalf("failed to create vetinf exporter: %s", err)
	}

	importStream := cli.ImportSession(context.Background())

	session, err := importer.NewManager(context.Background(), "vetinf", importStream)
	if err != nil {
		logrus.Fatalf("failed to create import manager: %s", err)
	}

	for customer := range stream {
		if customer.Deleted {
			// TODO(ppacher)
			logrus.Infof("vetinf: skipping deleted customer %s (%s %s)", customer.InternalRef, customer.LastName, customer.FirstName)
			continue
		}

		logrus.Infof("vetinf: upserting customer %s (%s %s)", customer.InternalRef, customer.LastName, customer.FirstName)

		if err := session.UpsertCustomerByRef(customer.InternalRef, customer.Customer); err != nil {
			logrus.Errorf("failed to upsert customer: %s", err)
		}
	}

	session.Stop()
}

func newInsecureClient() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
				// If you're also using this client for non-h2c traffic, you may want
				// to delegate to tls.Dial if the network isn't TCP or the addr isn't
				// in an allowlist.
				return net.Dial(network, addr)
			},
			// Don't forget timeouts!
		},
	}
}
