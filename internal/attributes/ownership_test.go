package attributes_test

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/stretchr/testify/assert"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/customer-service/internal/attributes"
	"github.com/tierklinik-dobersberg/customer-service/pkg/importer"
)

func TestAttributeUpdates(t *testing.T) {
	operations := []*customerv1.AttributeUpdate{
		{
			Kind:      customerv1.AttributeKind_ATTRIBUTE_KIND_FIRST_NAME,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_SET,
			Value: &customerv1.AttributeUpdate_StringValue{
				StringValue: "Firstname",
			},
		},
		{
			Kind:      customerv1.AttributeKind_ATTRIBUTE_KIND_LAST_NAME,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_SET,
			Value: &customerv1.AttributeUpdate_StringValue{
				StringValue: "Lastname",
			},
		},
		{
			Kind:      customerv1.AttributeKind_ATTRIBUTE_KIND_PHONE_NUMBER,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_ADD,
			Value: &customerv1.AttributeUpdate_StringValue{
				StringValue: "Phone1",
			},
		},
		{
			Kind:      customerv1.AttributeKind_ATTRIBUTE_KIND_PHONE_NUMBER,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_ADD,
			Value: &customerv1.AttributeUpdate_StringValue{
				StringValue: "Phone2",
			},
		},
		{
			Kind:      customerv1.AttributeKind_ATTRIBUTE_KIND_ADDRESS,
			Operation: customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_ADD,
			Value: &customerv1.AttributeUpdate_Address{
				Address: &customerv1.Address{
					PostalCode: "1234",
					City:       "City",
					Street:     "Street",
					Extra:      "Extra",
				},
			},
		},
	}

	ownedAttrs := make([]*customerv1.OwnedAttribute, len(operations))
	for idx, op := range operations {
		ownedAttrs[idx] = attributes.AttrUpdateToOwned(op)
	}

	testCase := struct {
		Importer       string
		Existing       *customerv1.Customer
		Operations     []*customerv1.AttributeUpdate
		States         []*customerv1.ImportState
		OutputCustomer *customerv1.Customer
		OutputState    *customerv1.ImportState
	}{
		Importer:   "test",
		Existing:   new(customerv1.Customer),
		Operations: operations,
		States:     []*customerv1.ImportState{},
		OutputCustomer: &customerv1.Customer{
			FirstName: "Firstname",
			LastName:  "Lastname",
			Addresses: []*customerv1.Address{
				{
					PostalCode: "1234",
					City:       "City",
					Street:     "Street",
					Extra:      "Extra",
				},
			},
			PhoneNumbers: []string{
				"Phone1",
				"Phone2",
			},
		},
		OutputState: &customerv1.ImportState{
			InternalReference: "test-ref",
			Importer:          "test",
			OwnedAttributes:   ownedAttrs,
		},
	}

	mng := attributes.NewManager("test-ref", testCase.Importer, testCase.Existing, testCase.States, false)

	for _, upd := range testCase.Operations {
		err := mng.ApplyUpdate(upd)
		assert.NoError(t, err)
	}

	assert.True(t, proto.Equal(testCase.OutputCustomer, mng.Customer))
	assert.True(t, proto.Equal(testCase.OutputState, mng.States[0]))
}

func TestWithDiffer(t *testing.T) {
	a := &customerv1.Customer{
		FirstName:    "foo",
		LastName:     "bar",
		PhoneNumbers: []string{"+4304", "+4305"},
	}

	output := &customerv1.Customer{
		FirstName: "Firstname",
		LastName:  "Lastname",
		Addresses: []*customerv1.Address{
			{
				PostalCode: "1234",
				City:       "City",
				Street:     "Street",
				Extra:      "Extra",
			},
		},
		PhoneNumbers: []string{
			"+4304",
			"+4302",
			"+4303",
		},
	}

	updates, err := importer.DefaultDiffer(a, output)

	output.FirstName = "foo"

	assert.NoError(t, err)
	ownedAttrs := make([]*customerv1.OwnedAttribute, 0, len(updates))
	for _, op := range updates {
		if op.Operation == customerv1.AttributeUpdateOperation_ATTRIBUTE_UPDATE_OPERATION_DELETE {
			continue
		}

		ownedAttrs = append(ownedAttrs, attributes.AttrUpdateToOwned(op))
	}

	mng := attributes.NewManager("test-ref", "test", a, []*customerv1.ImportState{
		{
			Importer: "test",
			OwnedAttributes: []*customerv1.OwnedAttribute{
				{
					Kind:  customerv1.AttributeKind_ATTRIBUTE_KIND_PHONE_NUMBER,
					Value: structpb.NewStringValue("+4304"),
				},
			},
		},
		{
			Importer: "user",
			OwnedAttributes: []*customerv1.OwnedAttribute{
				{
					Kind:   customerv1.AttributeKind_ATTRIBUTE_KIND_FIRST_NAME,
					Value:  structpb.NewStringValue("foo"),
					Ignore: true,
				},
			},
		},
	}, false)

	for _, upd := range updates {
		err := mng.ApplyUpdate(upd)
		assert.NoError(t, err)
	}

	assert.True(t, proto.Equal(output, a))
	assert.True(t, proto.Equal(mng.States[0], &customerv1.ImportState{
		InternalReference: "test-ref",
		Importer:          "test",
		OwnedAttributes:   ownedAttrs,
	}))
}
