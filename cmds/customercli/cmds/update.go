package cmds

import (
	"github.com/bufbuild/connect-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"github.com/tierklinik-dobersberg/customer-service/pkg/importer"
)

func GetUpdateCustomerCommand(root *cli.Root) *cobra.Command {
	customer := &customerv1.Customer{}

	cmd := &cobra.Command{
		Use:     "update id",
		Aliases: []string{"set"},
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cli := getClient(root)

			diff, err := importer.DefaultDiffer(new(customerv1.Customer), customer)
			if err != nil {
				logrus.Fatalf(err.Error())
			}

			res, err := cli.UpdateCustomer(root.Context(), connect.NewRequest(&customerv1.UpdateCustomerRequest{
				Id:      args[0],
				Updates: diff,
			}))
			if err != nil {
				logrus.Fatalf(err.Error())
			}

			root.Print(res.Msg)
		},
	}

	f := cmd.Flags()
	{
		f.StringVar(&customer.FirstName, "first-name", "", "")
		f.StringVar(&customer.LastName, "last-name", "", "")
	}

	return cmd
}
