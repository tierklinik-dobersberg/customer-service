package cmds

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"

	"github.com/bufbuild/connect-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1/customerv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/encoding/protojson"
)

func getClient(root *cli.Root) customerv1connect.CustomerServiceClient {
	return customerv1connect.NewCustomerServiceClient(newInsecureClient(), "http://localhost:8090")
}

func GetSearchCommand(root *cli.Root) *cobra.Command {
	var (
		names  []string
		phones []string
		mails  []string
		ids    []string
	)

	cmd := &cobra.Command{
		Use: "search [flags]",
		Run: func(cmd *cobra.Command, args []string) {
			cli := getClient(root)

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

			opts := protojson.MarshalOptions{
				Multiline: true,
				Indent:    "  ",
			}

			blob, err := opts.Marshal(res.Msg.Results[0])
			if err != nil {
				logrus.Fatal(err.Error())
			}

			vr, err := bsonrw.NewExtJSONValueReader(bytes.NewReader(blob), true)
			if err != nil {
				panic(err)
			}
			dec, err := bson.NewDecoder(vr)
			if err != nil {
				panic(err)
			}
			dec.DefaultDocumentM()

			var m bson.M
			if err := dec.Decode(&m); err != nil {
				logrus.Fatal(err.Error())
			}

			fmt.Printf("%#v\n", m)

			blob2, err := bson.MarshalExtJSON(m, true, false)
			if err != nil {
				logrus.Fatal(err.Error())
			}

			var c = new(customerv1.CustomerResponse)

			if err := protojson.Unmarshal(blob2, c); err != nil {
				logrus.Fatal(err.Error())
			}

			root.Print(c)
			//root.Print(res.Msg)
		},
	}

	f := cmd.Flags()
	{
		f.StringSliceVar(&names, "name", nil, "")
		f.StringSliceVar(&phones, "phone", nil, "")
		f.StringSliceVar(&mails, "mail", nil, "")
		f.StringSliceVar(&ids, "id", nil, "")
	}

	return cmd
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
