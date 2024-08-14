package importer

import (
	"errors"
	"fmt"

	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func AttrUpdateToOwned(update *customerv1.AttributeUpdate) *customerv1.OwnedAttribute {
	owned := &customerv1.OwnedAttribute{
		Kind: update.Kind,
	}

	switch v := update.Value.(type) {
	case *customerv1.AttributeUpdate_Address:
		spb, err := structpb.NewStruct(AddressToMap(v.Address))

		if err != nil {
			panic(err)
		}

		owned.Value = structpb.NewStructValue(spb)

	case *customerv1.AttributeUpdate_StringValue:
		owned.Value = structpb.NewStringValue(v.StringValue)
	}

	return owned
}

func OwnedToUpdate(owned *customerv1.OwnedAttribute) (*customerv1.AttributeUpdate, error) {
	update := &customerv1.AttributeUpdate{
		Kind: owned.Kind,
	}

	switch update.Kind {
	case customerv1.AttributeKind_ATTRIBUTE_KIND_ADDRESS:
		m := owned.Value.GetStructValue().AsMap()

		update.Value = &customerv1.AttributeUpdate_Address{
			Address: &customerv1.Address{
				PostalCode: m["postal_code"].(string),
				City:       m["city"].(string),
				Street:     m["street"].(string),
				Extra:      m["extra"].(string),
			},
		}

	case customerv1.AttributeKind_ATTRIBUTE_KIND_FIRST_NAME,
		customerv1.AttributeKind_ATTRIBUTE_KIND_LAST_NAME,
		customerv1.AttributeKind_ATTRIBUTE_KIND_EMAIL_ADDRESS,
		customerv1.AttributeKind_ATTRIBUTE_KIND_PHONE_NUMBER:

		update.Value = &customerv1.AttributeUpdate_StringValue{
			StringValue: owned.Value.GetStringValue(),
		}
	}

	return update, nil
}

func AddressToMap(addr *customerv1.Address) map[string]interface{} {
	return map[string]interface{}{
		"postal_code": addr.PostalCode,
		"city":        addr.City,
		"street":      addr.Street,
		"extra":       addr.Extra,
	}
}

func CompareAddresses(a, b *customerv1.Address) bool {
	return proto.Equal(a, b)
}

func CompareOwnedAttributes(a, b *customerv1.OwnedAttribute) bool {
	return proto.Equal(a, b)
}

func CustomerHasValue(customer *customerv1.Customer, owned *customerv1.OwnedAttribute) (bool, error) {
	switch owned.Kind {
	case customerv1.AttributeKind_ATTRIBUTE_KIND_ADDRESS:
		m := owned.Value.GetStructValue().AsMap()

		value, err := MapToAddress(m)
		if err != nil {
			return false, err
		}

		for _, addr := range customer.Addresses {
			if CompareAddresses(addr, value) {
				return true, nil
			}
		}

	case customerv1.AttributeKind_ATTRIBUTE_KIND_EMAIL_ADDRESS:
		value := owned.Value.GetStringValue()

		for _, mail := range customer.EmailAddresses {
			if mail == value {
				return true, nil
			}
		}

	case customerv1.AttributeKind_ATTRIBUTE_KIND_PHONE_NUMBER:
		value := owned.Value.GetStringValue()

		for _, phone := range customer.PhoneNumbers {
			if phone == value {
				return true, nil
			}
		}

	case customerv1.AttributeKind_ATTRIBUTE_KIND_FIRST_NAME:
		if owned.Value.GetStringValue() == customer.FirstName {
			return true, nil
		}

	case customerv1.AttributeKind_ATTRIBUTE_KIND_LAST_NAME:
		if owned.Value.GetStringValue() == customer.LastName {
			return true, nil
		}

	default:
		return false, fmt.Errorf("unsupported attribute kind: %s", owned.Kind.String())
	}

	return false, nil
}

var ErrExpectedString = errors.New("string expected")

func MapToAddress(m map[string]interface{}) (*customerv1.Address, error) {
	postalCode, ok := m["postal_code"].(string)
	if !ok {
		return nil, ErrExpectedString
	}

	city, ok := m["city"].(string)
	if !ok {
		return nil, ErrExpectedString
	}

	street, ok := m["street"].(string)
	if !ok {
		return nil, ErrExpectedString
	}

	extra, ok := m["extra"].(string)
	if !ok {
		return nil, ErrExpectedString
	}

	return &customerv1.Address{
		PostalCode: postalCode,
		Street:     street,
		City:       city,
		Extra:      extra,
	}, nil
}
