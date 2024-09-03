package cmds

import (
	"context"
	"fmt"
	"strings"

	"github.com/bufbuild/connect-go"
	"github.com/chzyer/readline"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
)

func GetSearchStreamCommand(root *cli.Root) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "stream",
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			cli := customerv1connect.NewCustomerServiceClient(cli.NewInsecureHttp2Client(), root.Config().BaseURLS.CustomerService)

			ctx, cancel := context.WithCancel(root.Context())
			defer cancel()

			stream := cli.SearchCustomerStream(ctx)

			rl, err := readline.New("> ")
			if err != nil {
				panic(err)
			}
			defer rl.Close()

			go func() {
				defer cancel()

				for {
					msg, err := stream.Receive()
					if err != nil {
						fmt.Fprintf(rl.Stderr(), "failed to receive from stream: %s\n", err)
						return
					}

					root.Print(msg.Results)
				}
			}()

		L:
			for {
				line, err := rl.Readline()
				if err != nil { // io.EOF
					break
				}

				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 1 {
					fmt.Fprintf(rl.Stderr(), "invalid search input\n")
				}

				req := &customerv1.SearchCustomerRequest{}

				switch parts[0] {
				case "name":
					req.Queries = []*customerv1.CustomerQuery{
						&customerv1.CustomerQuery{
							Query: &customerv1.CustomerQuery_Name{
								Name: &customerv1.NameQuery{
									LastName: parts[1],
								},
							},
						},
					}
				case "phone":
					req.Queries = []*customerv1.CustomerQuery{
						&customerv1.CustomerQuery{
							Query: &customerv1.CustomerQuery_PhoneNumber{
								PhoneNumber: parts[1],
							},
						},
					}
				case "mail":
					req.Queries = []*customerv1.CustomerQuery{
						&customerv1.CustomerQuery{
							Query: &customerv1.CustomerQuery_EmailAddress{
								EmailAddress: parts[1],
							},
						},
					}

				case "exist":
					break L
				default:
					fmt.Fprintf(rl.Stderr(), "invalid search input\n")
					continue L
				}

				if err := stream.Send(req); err != nil {
					fmt.Fprintf(rl.Stderr(), "failed to send request: %s\n", err.Error())
				}

			}

		},
	}

	return cmd
}

func GetSearchCommand(root *cli.Root) *cobra.Command {
	var (
		names    []string
		phones   []string
		mails    []string
		ids      []string
		analyze  bool
		pageSize int
		page     int
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

			if pageSize > 0 {
				req.Pagination = &commonv1.Pagination{
					PageSize: int32(pageSize),
					Kind: &commonv1.Pagination_Page{
						Page: int32(page),
					},
				}
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

	cmd.AddCommand(GetSearchStreamCommand(root))

	f := cmd.Flags()
	{
		f.StringSliceVar(&names, "name", nil, "")
		f.StringSliceVar(&phones, "phone", nil, "")
		f.StringSliceVar(&mails, "mail", nil, "")
		f.StringSliceVar(&ids, "id", nil, "")
		f.BoolVar(&analyze, "analyze", false, "Analyze customers")
		f.IntVar(&pageSize, "page-size", 0, "")
		f.IntVar(&page, "page", 0, "")
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
