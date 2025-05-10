package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nyaruka/phonenumbers"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/go-vetinf/vetinf"
	"google.golang.org/protobuf/types/known/structpb"
)

// ExportedCustomer is a customer exported from a VetInf
// installation.
type ExportedCustomer struct {
	*customerv1.Customer
	Deleted     bool
	InternalRef string
}

// ExportedPatient is a patient exported from a VetInf
// installation.
type ExportedPatient struct {
	*customerv1.Patient
	Deleted             bool
	InternalRef         string
	InternalCustomerRef string
}

// Exporter is capable of exporting and extracting
// data of a VetInf installation.
type Exporter struct {
	encoding   string
	regionCode string

	db      *vetinf.Infdat
	country string
}

type ImportResults struct {
	New      int
	Pristine int
	Updated  int
	Deleted  int
}

type VetInf struct {
	Directory string
	Encoding  string
}

// NewExporter creates a new exporter for vetinf.
func NewExporter(path, encoding, country, phonePrefix string) (*Exporter, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if !stat.IsDir() {
		return nil, fmt.Errorf("%q is not a directory", path)
	}

	if encoding == "" {
		encoding = "IBM852"
	}

	infdat := vetinf.OpenReadonlyFs(path, afero.NewOsFs())

	return &Exporter{
		db:         infdat,
		encoding:   encoding,
		country:    country,
		regionCode: phonePrefix,
	}, nil
}

// ExportCustomers exports all vetinf customers and streams them to
// the returned channel. Errors encountered when exporting single
// customers are logged and ignored.
func (e *Exporter) ExportCustomers(ctx context.Context) (<-chan *ExportedCustomer, int, error) {
	customerDB, err := e.db.CustomerDB(e.encoding)
	if err != nil {
		return nil, 0, err
	}

	dataCh, errCh, total := customerDB.StreamAll(ctx)

	customers := make(chan *ExportedCustomer, 10)

	go func() {
		for err := range errCh {
			logrus.Errorf("export: %s", err)
		}
	}()

	go func() {
		defer close(customers)
		for customer := range dataCh {
			if !isValidCustomer(&customer) {
				logrus.Infof("vetinf: skipping customer record: %+v", customer)
				continue
			}

			dbCustomer := &ExportedCustomer{
				Customer: &customerv1.Customer{
					FirstName: customer.Firstname,
					LastName:  customer.Name,
				},
				Deleted:     customer.Meta.Deleted,
				InternalRef: fmt.Sprintf("%d", customer.ID),
			}

			if customer.City != "" && customer.CityCode > 0 {
				dbCustomer.Customer.Addresses = append(dbCustomer.Customer.Addresses, &customerv1.Address{
					City:       customer.City,
					PostalCode: fmt.Sprintf("%d", customer.CityCode),
					Street:     customer.Street,
				})
			}

			key := e.regionCode

			var hasInvalidPhone bool

			if customer.Phone != "" {
				dbCustomer.PhoneNumbers = addNumber(key, dbCustomer.PhoneNumbers, customer.Phone, e.country, &hasInvalidPhone)
			}
			if customer.Phone2 != "" {
				dbCustomer.PhoneNumbers = addNumber(key, dbCustomer.PhoneNumbers, customer.Phone2, e.country, &hasInvalidPhone)
			}
			if customer.MobilePhone1 != "" {
				dbCustomer.PhoneNumbers = addNumber(key, dbCustomer.PhoneNumbers, customer.MobilePhone1, e.country, &hasInvalidPhone)
			}
			if customer.MobilePhone2 != "" {
				dbCustomer.PhoneNumbers = addNumber(key, dbCustomer.PhoneNumbers, customer.MobilePhone2, e.country, &hasInvalidPhone)
			}

			// add all possible mail addresses
			if customer.Mail != "" {
				dbCustomer.EmailAddresses = append(dbCustomer.EmailAddresses, customer.Mail)
			}

			if hasInvalidPhone {
				logrus.Infof("vetinf: customer %s has invalid phone number", key)
			}

			select {
			case customers <- dbCustomer:
			case <-ctx.Done():
				return
			}
		}
	}()

	return customers, total, nil
}

func addNumber(prefix string, numbers []string, number, country string, hasError *bool) []string {
	number = strings.TrimSpace(number)

	if number == "" {
		return numbers
	}

	if !strings.HasPrefix(number, "+") && !strings.HasPrefix(number, "0") && prefix != "" {
		number = prefix + number
	}

	p, err := phonenumbers.Parse(number, country)
	if err != nil {
		*hasError = true
		return numbers
	}

	return append(numbers, []string{
		phonenumbers.Format(p, phonenumbers.INTERNATIONAL),
	}...)
}

func isValidCustomer(c *vetinf.Customer) bool {
	if c == nil {
		return false
	}
	if c.ID == 0 {
		return false
	}
	return true
}

func isValidPatient(c *vetinf.SmallAnimalRecord) bool {
	if c == nil {
		return false
	}
	if c.AnimalID == "" {
		return false
	}
	if c.CustomerID == 0 {
		return false
	}

	return true
}

func (e *Exporter) ExportPatients(ctx context.Context) (<-chan *ExportedPatient, int, error) {
	animalDB, err := e.db.AnimalDB(e.encoding)
	if err != nil {
		return nil, 0, err
	}

	dataCh, errCh, total := animalDB.StreamAll(ctx)

	patients := make(chan *ExportedPatient, 10)

	go func() {
		for err := range errCh {
			logrus.Errorf("export: %s", err)
		}
	}()

	go func() {
		defer close(patients)
		for p := range dataCh {
			if !isValidPatient(&p) {
				logrus.Infof("vetinf: skipping patient record: %+v", p)
				continue
			}

			var bday *commonv1.Date
			if p.Birthday != "" {
				formats := []string{
					"02.01.2006",
					"2.1.2006",
					"02.1.2006",
					"2.01.2006",
					"01.2006",
					"1.2006",
					"2006",
				}

				for _, f := range formats {
					t, err := time.ParseInLocation(f, p.Birthday, time.Local)
					if err == nil {
						bday = commonv1.FromTime(t)
						break
					}
				}

				if bday == nil {
					logrus.Errorf("failed to parse birthday: %q", p.Birthday)
				}
			}

			var gender customerv1.PatientGender

			switch strings.ToLower(p.Gender) {
			case "m":
				gender = customerv1.PatientGender_PATIENT_GENDER_MALE
			case "mk":
				gender = customerv1.PatientGender_PATIENT_GENDER_MALE_CASTRATED
			case "f", "w":
				gender = customerv1.PatientGender_PATIENT_GENDER_FEMALE
			case "fk", "wk":
				gender = customerv1.PatientGender_PATIENT_GENDER_FEMALE_CASTRATED

			default:
				logrus.Infof("failed to get patient gender: %q", p.Gender)
				continue
			}

			additionalData := map[string]any{
				"extra1":  p.Extra1,
				"extra2":  p.Extra2,
				"extra3":  p.Extra3,
				"extra4":  p.Extra4,
				"extra5":  p.Extra5,
				"extra6":  p.Extra6,
				"extra7":  p.Extra7,
				"extra8":  p.Extra8,
				"extra9":  p.Extra9,
				"extra10": p.Extra10,
			}

			extra, err := structpb.NewStruct(additionalData)
			if err != nil {
				logrus.Infof("failed to prepare additional patient data: %s", err)
				continue
			}

			dbPatient := &ExportedPatient{
				Deleted:             p.Meta.Deleted,
				InternalRef:         p.AnimalID,
				InternalCustomerRef: fmt.Sprintf("%d", p.CustomerID),
				Patient: &customerv1.Patient{
					PatientName:      p.Size,
					Species:          p.Species,
					Birthday:         bday,
					Gender:           gender,
					Breed:            p.Breed,
					Comment:          p.SpecialDetail,
					CustomerId:       p.ChipNumber,
					Color:            p.Color,
					ExtraData:        extra,
					InternalRefernce: p.AnimalID,
				},
			}

			select {
			case patients <- dbPatient:
			case <-ctx.Done():
				return
			}
		}
	}()

	return patients, total, nil
}
