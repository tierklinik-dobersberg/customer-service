package carddav

import (
	"context"
	"fmt"
	"strings"

	"github.com/emersion/go-vcard"
	"github.com/emersion/go-webdav/carddav"
	"github.com/nyaruka/phonenumbers"
	"github.com/sirupsen/logrus"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/customer-service/pkg/importer"
)

func FindAddressBook(ctx context.Context, cli *Client, cfg *CardDAVConfig) error {
	if cfg.AddressBook != "" {
		return nil
	}

	logrus.Errorf("no address book configured. Trying to auto-detect the default addressbook")

	books, err := cli.ListAddressBooks(ctx)
	if err != nil {
		return fmt.Errorf("failed to enumerate address books: %w", err)
	}
	if len(books) == 0 {
		return fmt.Errorf("no address books available")
	}
	// try to find an address book with the name "default"
	for _, b := range books {
		if strings.ToLower(b.Name) == "default" {
			logrus.Infof("using address book %s (%s)", b.Name, b.Path)
			cfg.AddressBook = b.Path

			break
		}
	}
	if cfg.AddressBook == "" {
		b := books[0]
		logrus.Infof("using address book %s (%s)", b.Name, b.Path)

		cfg.AddressBook = b.Path
	}

	return nil
}

func ProcessUpdates(ctx context.Context, stream *importer.Manager, cfg *CardDAVConfig, deleted <-chan string, updated <-chan *carddav.AddressObject) error {
L:
	for {
		select {
		case <-deleted:
			// TODO(ppacher): not yet supported

		case upd, ok := <-updated:
			if !ok {
				break L
			}

			cus, ref, err := convertToCustomer(upd)
			if err != nil {
				logrus.Errorf("failed to convert address object to customer: %s: %s", upd.Path, err)

				continue
			}

			if err := stream.UpsertCustomerByRef(ref, cus, nil); err != nil {
				logrus.Errorf("failed to upsert customer: %s: %s", ref, err)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func convertToCustomer(ao *carddav.AddressObject) (*customerv1.Customer, string, error) {
	if ao.Card == nil {
		return nil, "", fmt.Errorf("no VCARD data available")
	}

	cus := new(customerv1.Customer)
	if n := ao.Card.Name(); n != nil {
		cus.FirstName = strings.TrimSpace(n.GivenName)
		cus.LastName = strings.TrimSpace(n.FamilyName)
	}

	if addr := ao.Card.Address(); addr != nil {
		cus.Addresses = append(cus.Addresses, &customerv1.Address{
			City:       strings.TrimSpace(addr.Locality),
			Street:     strings.TrimSpace(addr.StreetAddress),
			PostalCode: strings.TrimSpace(addr.PostalCode),
		})
	}

	cus.EmailAddresses = ao.Card.Values(vcard.FieldEmail)
	for _, phone := range ao.Card.Values(vcard.FieldTelephone) {
		number, err := phonenumbers.Parse(phone, "AT")
		if err != nil {
			logrus.Errorf("failed to parse phone number %q: %s", phone, err)

			continue
		}

		cus.PhoneNumbers = append(cus.PhoneNumbers,
			phonenumbers.Format(number, phonenumbers.INTERNATIONAL),
		)
	}

	return cus, ao.Card.Value(vcard.FieldUID), nil
}
