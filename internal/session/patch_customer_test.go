package session

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type resolver struct{}

func (r *resolver) IsAllowed(imported string, owners []string) bool {
	if imported == "test" {
		return true
	}

	return len(owners) == 0
}

func makeAddr(code, city, street string) *customerv1.Address {
	return &customerv1.Address{
		PostalCode: code,
		City:       city,
		Street:     street,
	}
}

func getCustomer(t *testing.T, importer, firstName, lastName string, phone []string, mail []string, addr []*customerv1.Address) (*customerv1.Customer, []*customerv1.ImportState) {
	imported := &customerv1.Customer{
		FirstName:      firstName,
		LastName:       lastName,
		Addresses:      addr,
		PhoneNumbers:   phone,
		EmailAddresses: mail,
	}

	patcher := NewPatcher(importer, "ref", new(resolver), new(customerv1.Customer), nil)

	require.NoError(t, patcher.Apply(imported), "applying changes should not return an erro")

	return patcher.Result, patcher.States
}

func Test_Patch_Empty(t *testing.T) {
	existing := &customerv1.Customer{}

	imported := &customerv1.Customer{
		FirstName: "first-name",
		LastName:  "last-name",
		Addresses: []*customerv1.Address{
			{
				Street:     "street",
				City:       "city",
				PostalCode: "postalCode",
			},
		},
		PhoneNumbers:   []string{"1234"},
		EmailAddresses: []string{"foo@example.com"},
	}

	output := repo.Clone(imported)

	patcher := NewPatcher("test", "ref", new(resolver), existing, nil)

	require.NotEmpty(t, patcher.States)
	require.NotNil(t, patcher.currentState)

	require.NoError(t, patcher.Apply(imported), "applying changes should not return an erro")
	require.True(t, proto.Equal(output, patcher.Result), "output object does not match")
	require.True(t, proto.Equal(patcher.currentState, &customerv1.ImportState{
		Importer: "test",
		OwnedAttributes: []*customerv1.OwnedAttribute{
			{
				Kind: &customerv1.OwnedAttribute_FirstName{
					FirstName: "first-name",
				},
			},
			{
				Kind: &customerv1.OwnedAttribute_LastName{
					LastName: "last-name",
				},
			},
			{
				Kind: &customerv1.OwnedAttribute_EmailAddress{
					EmailAddress: "foo@example.com",
				},
			},
			{
				Kind: &customerv1.OwnedAttribute_PhoneNumber{
					PhoneNumber: "1234",
				},
			},
			{
				Kind: &customerv1.OwnedAttribute_Address{
					Address: &customerv1.Address{
						Street:     "street",
						City:       "city",
						PostalCode: "postalCode",
					},
				},
			},
		},
	}))
}

func TestPruneAttributes(t *testing.T) {
	existingCustomer, existingStates := getCustomer(t, "test", "existing-firstname", "existing-lastname", []string{"1234"}, nil, nil)

	p := NewPatcher("test", "ref", new(resolver), existingCustomer, existingStates)
	require.NotEmpty(t, p.currentState.OwnedAttributes)

	require.NoError(t, p.Apply(new(customerv1.Customer)))
	require.Empty(t, p.currentState.OwnedAttributes)
}

func TestUpdatesSameImporter(t *testing.T) {
	existingCustomer, existingStates := getCustomer(t, "test", "existing-firstname", "existing-lastname", []string{"1234"}, nil, nil)
	updatedCustomer, expectedStates := getCustomer(t, "test", "existing-firstname", "", []string{"4321"}, []string{"example.com"}, []*customerv1.Address{
		makeAddr("1", "city", "street"),
	})

	p := NewPatcher("test", "ref", new(resolver), existingCustomer, existingStates)

	require.NoError(t, p.Apply(updatedCustomer))

	result := &customerv1.Customer{
		FirstName:      "existing-firstname",
		PhoneNumbers:   []string{"4321"},
		EmailAddresses: []string{"example.com"},
		Addresses: []*customerv1.Address{
			{
				PostalCode: "1",
				City:       "city",
				Street:     "street",
			},
		},
	}

	require.True(t, proto.Equal(p.Result, result))

	require.True(t, proto.Equal(p.currentState, expectedStates[0]))
}

func TestPriority(t *testing.T) {
	existingCustomer, existingStates := getCustomer(t, "test", "existing-firstname", "existing-lastname", []string{"1234"}, nil, nil)
	updatedCustomer, _ := getCustomer(t, "foo", "existing-firstname", "", []string{"4321"}, []string{"example.com"}, []*customerv1.Address{
		makeAddr("1", "city", "street"),
	})

	p := NewPatcher("foo", "ref", new(resolver), existingCustomer, existingStates)

	require.NoError(t, p.Apply(updatedCustomer))

	result := &customerv1.Customer{
		FirstName:      "existing-firstname",
		LastName:       "existing-lastname",
		PhoneNumbers:   []string{"1234", "4321"},
		EmailAddresses: []string{"example.com"},
		Addresses: []*customerv1.Address{
			{
				PostalCode: "1",
				City:       "city",
				Street:     "street",
			},
		},
	}

	require.Equal(t, toMap(result), toMap(p.Result))
}

func toMap(msg proto.Message) map[string]interface{} {
	blob, err := protojson.Marshal(msg)
	if err != nil {
		panic(err.Error())
	}

	var m map[string]interface{}

	if err := json.Unmarshal(blob, &m); err != nil {
		panic(err.Error())
	}

	return m
}
