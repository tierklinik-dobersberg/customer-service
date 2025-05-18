package main

import (
	"context"

	"github.com/bufbuild/connect-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
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

	logrus.Infof("creating http client")

	logrus.Infof("connecting to %s", root.Config().BaseURLS.CustomerService)

	/*
		root.HttpClient = h2utils.NewInsecureHttp2Client()

		cli := root.CustomerImport()

		importStream := cli.ImportSession(context.Background())

		session, err := importer.NewManager(context.Background(), "vetinf", importStream)
		if err != nil {
			logrus.Fatalf("failed to create import manager: %s", err)
		}
		defer session.Stop()

		customerStream, _, err := exporter.ExportCustomers(context.Background())
		if err != nil {
			logrus.Errorf("failed to create vetinf exporter: %s", err)
			return
		}
		for customer := range customerStream {
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

		patientStream, _, err := exporter.ExportPatients(context.Background())
		if err != nil {
			logrus.Errorf("failed to export patients: %s", err)
			return
		}

		for patient := range patientStream {
			if patient.Deleted {
				logrus.Infof("vetinf: skipping deleted patient %s (%s %s)", patient.InternalReference, patient.PatientName, patient.InternalCustomerRef)
				continue
			}

			if err := session.UpsertPatient(patient.InternalCustomerRef, patient.Patient); err != nil {
				logrus.Errorf("failed to upsert patient: %s", err)
			}
		}
	*/

	stream, err := exporter.ExportAnamnesis(context.TODO())
	if err != nil {
		logrus.Errorf("failed to export anamnesis: %s", err)
	}

	p := root.Patient()
	for msg := range stream {
		ref := msg.Reference.(*customerv1.AddAnamnesisRequest_PatientImportReference)

		if _, err := p.AddAnamnesis(context.Background(), connect.NewRequest(msg)); err != nil {
			logrus.Errorf("failed to persist anamnesis: %s, ref:%s %s", err, ref.PatientImportReference.Importer, ref.PatientImportReference.InternalReference)
		}
	}
}
