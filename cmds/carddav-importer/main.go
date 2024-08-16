package main

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"github.com/tierklinik-dobersberg/customer-service/cmds/carddav-importer/carddav"
	"github.com/tierklinik-dobersberg/customer-service/pkg/importer"
)

func main() {
	if err := getRootCmd().Execute(); err != nil {
		logrus.Fatalf(err.Error())
	}
}

func getRootCmd() *cli.Root {
	cfg := carddav.CardDAVConfig{}

	cmd := cli.New("carddav-importer")

	cmd.Run = func(_ *cobra.Command, args []string) {
		carddavCli, err := carddav.NewClient(context.Background(), &cfg)
		if err != nil {
			logrus.Fatal(err.Error())
		}

		customerCli := cmd.CustomerImport()

		stream := customerCli.ImportSession(context.Background())
		manager, err := importer.NewManager(context.Background(), "carddav", stream)
		if err != nil {
			logrus.Fatal(err.Error())
		}

		if err := carddav.FindAddressBook(context.Background(), carddavCli, &cfg); err != nil {
			logrus.Fatal(err.Error())
		}

		deleted, updated, _, err := carddavCli.Sync(context.Background(), cfg.AddressBook, "")
		if err != nil {
			logrus.Fatal(err.Error())
		}

		if err := carddav.ProcessUpdates(context.Background(), manager, &cfg, deleted, updated); err != nil {
			logrus.Fatal(err.Error())
		}
	}

	f := cmd.Flags()
	{
		f.StringVar(&cfg.Server, "carddav-server", "", "")
		f.StringVar(&cfg.User, "user", "", "")
		f.StringVar(&cfg.Password, "password", "", "")
		f.StringVar(&cfg.AddressBook, "address-book", "", "")
		f.BoolVar(&cfg.AllowInsecure, "insecure", false, "")
	}

	return cmd
}
