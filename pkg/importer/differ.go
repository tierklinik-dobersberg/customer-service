package importer

import customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"

type (
	Differ interface {
		Diff(old, new *customerv1.Customer) ([]*customerv1.AttributeUpdate, error)
	}

	DifferFunc func(old, new *customerv1.Customer) ([]*customerv1.AttributeUpdate, error)
)

func (df DifferFunc) Diff(a, b *customerv1.Customer) ([]*customerv1.AttributeUpdate, error) {
	return df(a, b)
}

var DefaultDiffer = DifferFunc(func(old, new *customerv1.Customer) ([]*customerv1.AttributeUpdate, error) {
	var result []*customerv1.AttributeUpdate

	if old.FirstName != new.FirstName {
		result = append(result, &customerv1.AttributeUpdate{
			Kind:      customerv1.AttributeKind_ATTRIBUTE_KIND_FIRST_NAME,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_SET,
			Value: &customerv1.AttributeUpdate_StringValue{
				StringValue: new.FirstName,
			},
		})
	}

	if old.LastName != new.LastName {
		result = append(result, &customerv1.AttributeUpdate{
			Kind:      customerv1.AttributeKind_ATTRIBUTE_KIND_LAST_NAME,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_SET,
			Value: &customerv1.AttributeUpdate_StringValue{
				StringValue: new.LastName,
			},
		})
	}

	// Find attribute operations for the email address list
	mailDiff, err := diffStringList(old.EmailAddresses, new.EmailAddresses, customerv1.AttributeKind_ATTRIBUTE_KIND_EMAIL_ADDRESS)
	if err != nil {
		return nil, err
	}
	result = append(result, mailDiff...)

	// Find attribute operations for the phone number list
	phoneDiff, err := diffStringList(old.PhoneNumbers, new.PhoneNumbers, customerv1.AttributeKind_ATTRIBUTE_KIND_PHONE_NUMBER)
	if err != nil {
		return nil, err
	}
	result = append(result, phoneDiff...)

	// Find attribute operations for the address list
	addressDiff, err := diffAddressList(old.Addresses, new.Addresses)
	if err != nil {
		return nil, err
	}
	result = append(result, addressDiff...)

	return result, nil
})

func diffStringList(old []string, new []string, kind customerv1.AttributeKind) ([]*customerv1.AttributeUpdate, error) {
	var result []*customerv1.AttributeUpdate

	for _, oldEntry := range old {
		found := false
		for _, newEntry := range new {
			if oldEntry == newEntry {
				found = true
				break
			}
		}

		if found {
			continue
		}

		result = append(result, &customerv1.AttributeUpdate{
			Kind:      kind,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_DELETE,
			Value: &customerv1.AttributeUpdate_StringValue{
				StringValue: oldEntry,
			},
		})
	}

	for _, newEntry := range new {
		found := false
		for _, oldEntry := range old {
			if oldEntry == newEntry {
				found = true
				break
			}
		}

		if found {
			continue
		}

		result = append(result, &customerv1.AttributeUpdate{
			Kind:      kind,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_ADD,
			Value: &customerv1.AttributeUpdate_StringValue{
				StringValue: newEntry,
			},
		})
	}

	return result, nil
}

func diffAddressList(old []*customerv1.Address, new []*customerv1.Address) ([]*customerv1.AttributeUpdate, error) {
	var result []*customerv1.AttributeUpdate

	cmp := func(a *customerv1.Address, b *customerv1.Address) bool {
		return a.City == b.City && a.PostalCode == b.PostalCode && a.Street == b.Street && a.Extra == b.Extra
	}

	for _, oldEntry := range old {
		found := false
		for _, newEntry := range new {
			if cmp(oldEntry, newEntry) {
				found = true
				break
			}
		}

		if found {
			continue
		}

		result = append(result, &customerv1.AttributeUpdate{
			Kind:      customerv1.AttributeKind_ATTRIBUTE_KIND_ADDRESS,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_DELETE,
			Value: &customerv1.AttributeUpdate_Address{
				Address: oldEntry,
			},
		})
	}

	for _, newEntry := range new {
		found := false
		for _, oldEntry := range old {
			if cmp(oldEntry, newEntry) {
				found = true
				break
			}
		}

		if found {
			continue
		}

		result = append(result, &customerv1.AttributeUpdate{
			Kind:      customerv1.AttributeKind_ATTRIBUTE_KIND_ADDRESS,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_ADD,
			Value: &customerv1.AttributeUpdate_Address{
				Address: newEntry,
			},
		})
	}

	return result, nil
}
