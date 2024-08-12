package main

import (
	"github.com/sirupsen/logrus"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"github.com/tierklinik-dobersberg/customer-service/cmds/customercli/cmds"
)

func main() {
	cmd := cli.New("customercli")

	cmd.AddCommand(
		cmds.GetSearchCommand(cmd),
		cmds.GetUpdateCustomerCommand(cmd),
	)

	if err := cmd.Execute(); err != nil {
		logrus.Fatal(err.Error())
	}
}
