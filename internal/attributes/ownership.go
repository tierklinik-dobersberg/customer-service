package attributes

import (
	"fmt"

	"github.com/nyaruka/phonenumbers"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type Manager struct {
	Customer *customerv1.Customer
	States   []*customerv1.ImportState
	Importer string

	currentState *customerv1.ImportState
	setIgnore    bool
}

func NewManager(ref, importer string, customer *customerv1.Customer, states []*customerv1.ImportState, setIngore bool) *Manager {
	if customer == nil {
		customer = &customerv1.Customer{}
	}

	mng := &Manager{
		Customer:  customer,
		States:    states,
		Importer:  importer,
		setIgnore: setIngore,
	}

	for _, state := range states {
		if state.Importer == importer {
			mng.currentState = state
			break
		}
	}

	if mng.currentState == nil {
		mng.currentState = &customerv1.ImportState{
			Importer: importer,
		}

		mng.States = append(mng.States, mng.currentState)
	}

	mng.currentState.InternalReference = ref

	return mng
}

func (am *Manager) ApplyUpdate(update *customerv1.AttributeUpdate) error {
	owned := AttrUpdateToOwned(update)

	if am.setIgnore {
		owned.Ignore = true
	}

	if am.isAttributeIngored(owned) {
		return nil
	}

	switch update.Kind {
	case customerv1.AttributeKind_ATTRIBUTE_KIND_ADDRESS:
	case customerv1.AttributeKind_ATTRIBUTE_KIND_PHONE_NUMBER:
	case customerv1.AttributeKind_ATTRIBUTE_KIND_EMAIL_ADDRESS:
	case customerv1.AttributeKind_ATTRIBUTE_KIND_FIRST_NAME:
	case customerv1.AttributeKind_ATTRIBUTE_KIND_LAST_NAME:

	case customerv1.AttributeKind_ATTRIBUTE_KIND_UNSPECIFIED:
		fallthrough
	default:
		return fmt.Errorf("unknown or unspecified attribute kind: %s", update.Kind.String())
	}

	switch update.Operation {
	case customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_ADD:
		var added bool
		am.currentState.OwnedAttributes, added = addToSet(am.currentState.OwnedAttributes, owned, CompareOwnedAttributes)

		if added {
			switch update.Kind {
			case customerv1.AttributeKind_ATTRIBUTE_KIND_ADDRESS:
				sv, ok := update.Value.(*customerv1.AttributeUpdate_Address)
				if !ok {
					return fmt.Errorf("invalid value for addresses")
				}

				am.Customer.Addresses, _ = addToSet(am.Customer.Addresses, sv.Address, CompareAddresses)

			case customerv1.AttributeKind_ATTRIBUTE_KIND_PHONE_NUMBER:
				sv, ok := update.Value.(*customerv1.AttributeUpdate_StringValue)
				if !ok {
					return fmt.Errorf("invalid value for phone_numbers")
				}

				number, err := phonenumbers.Parse(sv.StringValue, "AT")
				if err != nil {
					return fmt.Errorf("invalid phone number: %w", err)
				}

				formatted := phonenumbers.Format(number, phonenumbers.INTERNATIONAL)

				am.Customer.PhoneNumbers, _ = addToSet(am.Customer.PhoneNumbers, formatted, func(a, b string) bool {
					return a == b
				})

			case customerv1.AttributeKind_ATTRIBUTE_KIND_EMAIL_ADDRESS:
				sv, ok := update.Value.(*customerv1.AttributeUpdate_StringValue)
				if !ok {
					return fmt.Errorf("invalid value for email_addresses")
				}

				am.Customer.EmailAddresses, _ = addToSet(am.Customer.EmailAddresses, sv.StringValue, func(a, b string) bool {
					return a == b
				})

			default:
				return fmt.Errorf("unknown or unspecified attribute kind: %s", update.Kind.String())
			}
		}

	case customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_DELETE:
		var deleted bool
		am.currentState.OwnedAttributes, deleted = deleteFromSet(am.currentState.OwnedAttributes, owned, CompareOwnedAttributes)

		if deleted && !am.attributeStillOwned(owned) {
			switch update.Kind {
			case customerv1.AttributeKind_ATTRIBUTE_KIND_ADDRESS:
				sv, ok := update.Value.(*customerv1.AttributeUpdate_Address)
				if !ok {
					return fmt.Errorf("invalid value for addresses")
				}

				am.Customer.Addresses, _ = deleteFromSet(am.Customer.Addresses, sv.Address, CompareAddresses)

			case customerv1.AttributeKind_ATTRIBUTE_KIND_PHONE_NUMBER:
				sv, ok := update.Value.(*customerv1.AttributeUpdate_StringValue)
				if !ok {
					return fmt.Errorf("invalid value for phone_numbers")
				}

				am.Customer.PhoneNumbers, _ = deleteFromSet(am.Customer.PhoneNumbers, sv.StringValue, func(a, b string) bool {
					return a == b
				})

			case customerv1.AttributeKind_ATTRIBUTE_KIND_EMAIL_ADDRESS:
				sv, ok := update.Value.(*customerv1.AttributeUpdate_StringValue)
				if !ok {
					return fmt.Errorf("invalid value for email_addresses")
				}

				am.Customer.EmailAddresses, _ = deleteFromSet(am.Customer.EmailAddresses, sv.StringValue, func(a, b string) bool {
					return a == b
				})

			default:
				return fmt.Errorf("unknown or unspecified attribute kind: %s", update.Kind.String())
			}
		}

	case customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_SET:
		if !am.attributeSettable(owned) {
			return nil
		}

		am.currentState.OwnedAttributes, _ = addToSet(am.currentState.OwnedAttributes, owned, CompareOwnedAttributes)

		switch update.Kind {
		case customerv1.AttributeKind_ATTRIBUTE_KIND_FIRST_NAME:
			sv, ok := update.Value.(*customerv1.AttributeUpdate_StringValue)
			if !ok {
				return fmt.Errorf("invalid value for first_name")
			}

			am.Customer.FirstName = sv.StringValue

		case customerv1.AttributeKind_ATTRIBUTE_KIND_LAST_NAME:
			sv, ok := update.Value.(*customerv1.AttributeUpdate_StringValue)
			if !ok {
				return fmt.Errorf("invalid value for last_name")
			}

			am.Customer.LastName = sv.StringValue

		default:
			return fmt.Errorf("unknown or unspecified attribute kind: %s", update.Kind.String())
		}
	}

	return nil
}

func (am *Manager) isAttributeIngored(owned *customerv1.OwnedAttribute) bool {
	for _, state := range am.States {
		for _, attr := range state.OwnedAttributes {
			if CompareOwnedAttributes(attr, owned) && attr.Ignore && state.Importer != am.Importer {
				return true
			}
		}
	}

	return false
}

func (am *Manager) attributeSettable(owned *customerv1.OwnedAttribute) bool {
	if am.isAttributeIngored(owned) {
		return false
	}

	for _, state := range am.States {
		for _, attr := range state.OwnedAttributes {
			if CompareOwnedAttributes(attr, owned) {
				return state.Importer == am.Importer
			}
		}
	}

	return true
}

func (am *Manager) attributeStillOwned(owned *customerv1.OwnedAttribute) bool {
	for _, state := range am.States {
		for _, attr := range state.OwnedAttributes {
			if CompareOwnedAttributes(attr, owned) {
				return true
			}
		}
	}

	return false
}

func CompareOwnedAttributes(a, b *customerv1.OwnedAttribute) bool {
	return proto.Equal(a, b)
}

func addToSet[E any, T []E](list T, element E, cmp func(E, E) bool) (T, bool) {
	for _, e := range list {
		if cmp(e, element) {
			return list, false
		}
	}

	return append(list, element), true
}

func deleteFromSet[E any, T []E](list T, element E, cmp func(E, E) bool) (T, bool) {
	for idx, e := range list {
		if cmp(e, element) {
			return append(list[:idx], list[idx+1:]...), true
		}
	}

	return list, false
}

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
