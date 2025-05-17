package models

import (
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Address struct {
	PostalCode string `bson:"postalCode"`
	City       string `bson:"city"`
	Street     string `bson:"street"`
	Extra      string `bson:"extra"`
}

func (a Address) ToProto() *customerv1.Address {
	return &customerv1.Address{
		PostalCode: a.PostalCode,
		City:       a.City,
		Street:     a.Street,
		Extra:      a.Extra,
	}
}

func AddressFromProto(apb *customerv1.Address) Address {
	return Address{
		PostalCode: apb.PostalCode,
		City:       apb.City,
		Street:     apb.Street,
		Extra:      apb.Extra,
	}
}

type Customer struct {
	FirstName       string    `bson:"firstName"`
	LastName        string    `bson:"lastName"`
	Addresses       []Address `bson:"addresses"`
	PhoneNumbers    []string  `bson:"phoneNumbers"`
	EmailAddresses  []string  `bson:"emailAddresses"`
	RecordCreatedAt time.Time `bson:"recordCreatedAt"`
}

func CustomerFromProto(cpb *customerv1.Customer) (Customer, error) {
	c := Customer{
		FirstName:      cpb.FirstName,
		LastName:       cpb.LastName,
		PhoneNumbers:   cpb.PhoneNumbers,
		EmailAddresses: cpb.EmailAddresses,
		Addresses:      make([]Address, len(cpb.Addresses)),
	}

	for idx, a := range cpb.Addresses {
		c.Addresses[idx] = AddressFromProto(a)
	}

	if cpb.RecordCreatedAt.IsValid() {
		c.RecordCreatedAt = cpb.RecordCreatedAt.AsTime()
	}

	return c, nil
}

func (c Customer) ToProto() *customerv1.Customer {
	cpb := &customerv1.Customer{
		FirstName:       c.FirstName,
		LastName:        c.LastName,
		PhoneNumbers:    c.PhoneNumbers,
		EmailAddresses:  c.EmailAddresses,
		RecordCreatedAt: timestamppb.New(c.RecordCreatedAt),
		Addresses:       make([]*customerv1.Address, len(c.Addresses)),
	}

	for idx, a := range c.Addresses {
		cpb.Addresses[idx] = a.ToProto()
	}

	return cpb
}

type OwnedAttribute struct {
	FirstName    string   `bson:"firstName"`
	LastName     string   `bson:"lastName"`
	PhoneNumber  string   `bson:"phoneNumber"`
	EmailAddress string   `bson:"emailAddress"`
	Address      *Address `bson:"address"`
}

func OwnedAttributeFromProto(opb *customerv1.OwnedAttribute) OwnedAttribute {
	switch v := opb.Kind.(type) {
	case *customerv1.OwnedAttribute_FirstName:
		return OwnedAttribute{
			FirstName: v.FirstName,
		}

	case *customerv1.OwnedAttribute_LastName:
		return OwnedAttribute{
			LastName: v.LastName,
		}

	case *customerv1.OwnedAttribute_PhoneNumber:
		return OwnedAttribute{
			PhoneNumber: v.PhoneNumber,
		}

	case *customerv1.OwnedAttribute_EmailAddress:
		return OwnedAttribute{
			EmailAddress: v.EmailAddress,
		}

	case *customerv1.OwnedAttribute_Address:
		a := AddressFromProto(v.Address)
		return OwnedAttribute{
			Address: &a,
		}

	default:
		return OwnedAttribute{}
	}
}

func (o OwnedAttribute) ToProto() *customerv1.OwnedAttribute {
	switch {
	case o.FirstName != "":
		return &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_FirstName{
				FirstName: o.FirstName,
			},
		}

	case o.LastName != "":
		return &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_LastName{
				LastName: o.LastName,
			},
		}

	case o.PhoneNumber != "":
		return &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_PhoneNumber{
				PhoneNumber: o.PhoneNumber,
			},
		}

	case o.EmailAddress != "":
		return &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_EmailAddress{
				EmailAddress: o.EmailAddress,
			},
		}

	case o.Address != nil:
		return &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_Address{
				Address: o.Address.ToProto(),
			},
		}

	default:
		return nil
	}
}

type ImportState struct {
	Importer          string           `bson:"importer"`
	LastSeen          time.Time        `bson:"lastSeen"`
	ExtraData         map[string]any   `bson:"extraData"`
	InternalReference string           `bson:"internalReference"`
	OwnedAttributes   []OwnedAttribute `bson:"ownedAttributes"`
}

func ImportStateFromProto(ipb *customerv1.ImportState) ImportState {
	i := ImportState{
		Importer:          ipb.Importer,
		InternalReference: ipb.InternalReference,
		OwnedAttributes:   make([]OwnedAttribute, len(ipb.OwnedAttributes)),
	}

	if ipb.LastSeen.IsValid() {
		i.LastSeen = ipb.LastSeen.AsTime()
	}

	if ipb.ExtraData != nil {
		i.ExtraData = ipb.ExtraData.AsMap()
	}

	for idx, a := range ipb.OwnedAttributes {
		i.OwnedAttributes[idx] = OwnedAttributeFromProto(a)
	}

	return i
}

func (s ImportState) ToProto() (*customerv1.ImportState, error) {
	spb := &customerv1.ImportState{
		Importer:          s.Importer,
		LastSeen:          timestamppb.New(s.LastSeen),
		InternalReference: s.InternalReference,
		OwnedAttributes:   make([]*customerv1.OwnedAttribute, len(s.OwnedAttributes)),
	}

	for idx, a := range s.OwnedAttributes {
		spb.OwnedAttributes[idx] = a.ToProto()
	}

	if s.ExtraData != nil {
		val, err := structpb.NewStruct(s.ExtraData)
		if err != nil {
			return spb, err
		}

		spb.ExtraData = val
	}

	return spb, nil
}

type CustomerAndState struct {
	ID       primitive.ObjectID `bson:"_id"`
	Customer Customer           `bson:"customer"`
	States   []ImportState      `bson:"states"`
}

func CustomerAndStateFromProto(cspb *customerv1.CustomerResponse) (CustomerAndState, error) {
	cs := CustomerAndState{}

	if cspb.Customer.Id != "" {
		oid, err := primitive.ObjectIDFromHex(cspb.Customer.Id)
		if err != nil {
			return cs, fmt.Errorf("invalid customer id: %w", err)
		}

		cs.ID = oid
	}

	customer, err := CustomerFromProto(cspb.Customer)
	if err != nil {
		return cs, err
	}
	cs.Customer = customer

	cs.States = make([]ImportState, len(cspb.States))

	merr := new(multierror.Error)
	for idx, s := range cspb.States {
		spb := ImportStateFromProto(s)
		if err != nil {
			merr.Errors = append(merr.Errors, err)
		}

		cs.States[idx] = spb
	}

	return cs, nil
}

func (cs CustomerAndState) ToProto() (*customerv1.CustomerResponse, error) {
	r := &customerv1.CustomerResponse{
		Customer: cs.Customer.ToProto(),
		States:   make([]*customerv1.ImportState, 0, len(cs.States)),
	}

	r.Customer.Id = cs.ID.Hex()

	var merr = new(multierror.Error)
	for _, state := range cs.States {
		spb, err := state.ToProto()
		if err != nil {
			merr.Errors = append(merr.Errors, err)
			continue
		}

		r.States = append(r.States, spb)
	}

	return r, merr.ErrorOrNil()
}
