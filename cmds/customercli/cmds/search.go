package cmds

import (
	"fmt"
	"strings"

	"github.com/bufbuild/connect-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
)

func GetSearchCommand(root *cli.Root) *cobra.Command {
	var (
		names   []string
		phones  []string
		mails   []string
		ids     []string
		analyze bool
	)

	cmd := &cobra.Command{
		Use: "search [flags]",
		Run: func(cmd *cobra.Command, args []string) {
			cli := root.Customer()

			req := &customerv1.SearchCustomerRequest{}

			for _, name := range names {
				req.Queries = append(req.Queries, &customerv1.CustomerQuery{
					Query: &customerv1.CustomerQuery_Name{
						Name: &customerv1.NameQuery{
							LastName: name,
						},
					},
				})
			}

			for _, phone := range phones {
				req.Queries = append(req.Queries, &customerv1.CustomerQuery{
					Query: &customerv1.CustomerQuery_PhoneNumber{
						PhoneNumber: phone,
					},
				})
			}

			for _, mail := range mails {
				req.Queries = append(req.Queries, &customerv1.CustomerQuery{
					Query: &customerv1.CustomerQuery_EmailAddress{
						EmailAddress: mail,
					},
				})
			}

			for _, id := range ids {
				req.Queries = append(req.Queries, &customerv1.CustomerQuery{
					Query: &customerv1.CustomerQuery_Id{
						Id: id,
					},
				})
			}

			res, err := cli.SearchCustomer(root.Context(), connect.NewRequest(req))
			if err != nil {
				logrus.Fatalf(err.Error())
			}

			if !analyze {
				root.Print(res.Msg)
			} else {
				analyzeCustomers(res.Msg.Results)
			}
		},
	}

	f := cmd.Flags()
	{
		f.StringSliceVar(&names, "name", nil, "")
		f.StringSliceVar(&phones, "phone", nil, "")
		f.StringSliceVar(&mails, "mail", nil, "")
		f.StringSliceVar(&ids, "id", nil, "")
		f.BoolVar(&analyze, "analyze", false, "Analyze customers")
	}

	return cmd
}

func analyzeCustomers(list []*customerv1.CustomerResponse) {
	var (
		countByPostalCode     = make(map[string]int)
		cityNamesByPostalCode = make(map[string][]string)
	)

	for _, c := range list {
		for _, addr := range c.Customer.Addresses {
			countByPostalCode[addr.PostalCode]++

			found := false
			for _, cityName := range cityNamesByPostalCode[addr.PostalCode] {
				if cityName == addr.City {
					found = true
					break
				}
			}

			if !found {
				cityNamesByPostalCode[addr.PostalCode] = append(cityNamesByPostalCode[addr.PostalCode], addr.City)
			}
		}
	}

	fmt.Printf("Total Customers: %d\n", len(list))
	fmt.Println("Postal-Codes:")

	for code, count := range countByPostalCode {
		fmt.Printf("\t%s: %d (", code, count)
		fmt.Print(strings.Join(cityNamesByPostalCode[code], ", "))
		fmt.Print(")\n")
	}
}
