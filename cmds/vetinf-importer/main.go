package main

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"github.com/tierklinik-dobersberg/customer-service/pkg/importer"
)

var (
	encoding           string
	defaultPhonePrefix string
)

func main() {
	if err := getRootCommand().Execute(); err != nil {
		logrus.Fatal(err)
	}
}

func getRootCommand() *cli.Root {
	cmd := cli.New("vetinf-importer [path/to/Infdat]")

	cmd.Run = func(_ *cobra.Command, args []string) {
		execute(cmd, args)
	}

	cmd.Args = cobra.ExactArgs(1)

	f := cmd.Flags()
	{
		f.StringVar(&encoding, "encoding", "IBM852", "The encoding of the VetInf database")
		f.StringVar(&defaultPhonePrefix, "phone-prefix", "", "The default phone region code")
	}

	cmd.MarkFlagRequired("server")

	return cmd
}

func execute(root *cli.Root, args []string) {
	exporter, err := NewExporter(args[0], encoding, "AT", defaultPhonePrefix)
	if err != nil {
		logrus.Fatalf("failed to create vetinf exporter: %s", err)
	}

	cli := root.CustomerImport()

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

		if err := session.UpsertCustomerByRef(customer.InternalRef, customer.Customer, nil); err != nil {
			logrus.Errorf("failed to upsert customer: %s", err)
		}
	}

	session.Stop()
}
