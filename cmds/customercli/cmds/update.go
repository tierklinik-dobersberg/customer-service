package cmds

import (
	"github.com/spf13/cobra"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
)

func GetUpdateCustomerCommand(root *cli.Root) *cobra.Command {
	customer := &customerv1.Customer{}

	cmd := &cobra.Command{
		Use:     "update id",
		Aliases: []string{"set"},
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
		},
	}

	f := cmd.Flags()
	{
		f.StringVar(&customer.FirstName, "first-name", "", "")
		f.StringVar(&customer.LastName, "last-name", "", "")
	}

	return cmd
}
