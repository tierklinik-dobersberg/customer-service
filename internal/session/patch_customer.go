package session

import (
	"fmt"
	"log/slog"

	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/customer-service/internal/repo"
	"google.golang.org/protobuf/proto"
)

type PriorityResolver interface {
	IsAllowed(newImporter string, existingImporters []string) bool
}

type Patcher struct {
	Importer string
	Ref      string

	Existing *customerv1.Customer
	Result   *customerv1.Customer

	resolver PriorityResolver

	States       []*customerv1.ImportState
	currentState *customerv1.ImportState
}

func NewPatcher(importer, ref string, resolver PriorityResolver, existing *customerv1.Customer, states []*customerv1.ImportState) *Patcher {
	if existing == nil {
		existing = new(customerv1.Customer)
	}

	result := repo.Clone(existing)

	var statesCopy []*customerv1.ImportState
	for _, s := range states {
		statesCopy = append(statesCopy, repo.Clone(s))
	}

	// now, find the importer state (or create a new one)
	statesCopy, currentState := findImporterState(importer, ref, states)

	p := &Patcher{
		Existing:     existing,
		Result:       result,
		States:       statesCopy,
		Importer:     importer,
		Ref:          ref,
		currentState: currentState,
		resolver:     resolver,
	}

	return p
}

func (p *Patcher) canSet(owners []string) bool {
	return p.resolver.IsAllowed(p.Importer, owners)
}

func (p *Patcher) Apply(importedCustomer *customerv1.Customer) error {
	if err := p.applyFirstName(importedCustomer); err != nil {
		return fmt.Errorf("first_name: %w", err)
	}

	if err := p.applyLastName(importedCustomer); err != nil {
		return fmt.Errorf("last_name: %w", err)
	}

	if err := p.applyStringList(&p.Result.EmailAddresses, importedCustomer.EmailAddresses, func(value string) *customerv1.OwnedAttribute {
		return &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_EmailAddress{
				EmailAddress: value,
			},
		}
	}); err != nil {
		return fmt.Errorf("email_addresses: %w", err)
	}

	if err := p.applyStringList(&p.Result.PhoneNumbers, importedCustomer.PhoneNumbers, func(value string) *customerv1.OwnedAttribute {
		return &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_PhoneNumber{
				PhoneNumber: value,
			},
		}
	}); err != nil {
		return fmt.Errorf("phone_numbers: %w", err)
	}

	if err := p.applyAddressList(importedCustomer); err != nil {
		return fmt.Errorf("addresses: %w", err)
	}

	// next, we prune all owned-attribute for this importer where the value is not present
	// on the imported customer anymore.
	if err := p.pruneAttributes(importedCustomer); err != nil {
		return err
	}

	// finally, we need to clean-up the result by removing all elements that do not have
	// any owned attributes anymore.
	if err := p.cleanResult(); err != nil {
		return fmt.Errorf("cleaning: %w", err)
	}

	return nil
}

func (p *Patcher) cleanResult() error {
	if p.Result.FirstName != "" {
		owned := &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_FirstName{
				FirstName: p.Result.FirstName,
			},
		}

		owners, _, err := p.FindAttributeOwners(owned)
		if err != nil {
			return err
		}

		if len(owners) == 0 {
			p.Result.FirstName = ""
		}
	}

	if p.Result.LastName != "" {
		owned := &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_LastName{
				LastName: p.Result.LastName,
			},
		}

		owners, _, err := p.FindAttributeOwners(owned)
		if err != nil {
			return err
		}

		if len(owners) == 0 {
			p.Result.LastName = ""
		}
	}

	var newMails []string
	for _, mail := range p.Result.EmailAddresses {
		owned := &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_EmailAddress{
				EmailAddress: mail,
			},
		}

		owners, _, err := p.FindAttributeOwners(owned)
		if err != nil {
			return err
		}

		if len(owners) > 0 {
			newMails = append(newMails, mail)
		}
	}
	p.Result.EmailAddresses = newMails

	var newPhone []string
	for _, phone := range p.Result.PhoneNumbers {
		owned := &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_PhoneNumber{
				PhoneNumber: phone,
			},
		}

		owners, _, err := p.FindAttributeOwners(owned)
		if err != nil {
			return err
		}

		if len(owners) > 0 {
			newPhone = append(newPhone, phone)
		}
	}
	p.Result.PhoneNumbers = newPhone

	var newAddrs []*customerv1.Address
	for _, addr := range p.Result.Addresses {
		owned := &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_Address{
				Address: addr,
			},
		}

		owners, _, err := p.FindAttributeOwners(owned)
		if err != nil {
			return err
		}

		if len(owners) > 0 {
			newAddrs = append(newAddrs, addr)
		}
	}
	p.Result.Addresses = newAddrs

	return nil
}

func (p *Patcher) pruneAttributes(importedCustomer *customerv1.Customer) error {
	var newList []*customerv1.OwnedAttribute

	for _, existingOwnedAttr := range p.currentState.OwnedAttributes {
		keep := false

		switch v := existingOwnedAttr.Kind.(type) {
		case *customerv1.OwnedAttribute_FirstName:
			keep = importedCustomer.FirstName == v.FirstName

		case *customerv1.OwnedAttribute_LastName:
			keep = importedCustomer.LastName == v.LastName

		case *customerv1.OwnedAttribute_EmailAddress:
			for _, val := range importedCustomer.EmailAddresses {
				if val == v.EmailAddress {
					keep = true
					break
				}
			}

		case *customerv1.OwnedAttribute_PhoneNumber:
			for _, val := range importedCustomer.PhoneNumbers {
				if val == v.PhoneNumber {
					keep = true
					break
				}
			}

		case *customerv1.OwnedAttribute_Address:
			for _, val := range importedCustomer.Addresses {
				if proto.Equal(val, v.Address) {
					keep = true
					break
				}
			}

		default:
			return fmt.Errorf("unsupported owned attribute kind: %T", v)
		}

		if keep {
			newList = append(newList, existingOwnedAttr)
		} else {
			slog.Info("pruning attribute", slog.String("importer", p.Importer), slog.Any("attribute", existingOwnedAttr))
		}
	}

	p.currentState.OwnedAttributes = newList

	return nil
}

func (p *Patcher) applyFirstName(importedCustomer *customerv1.Customer) error {
	if importedCustomer.FirstName != "" {
		owned := &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_FirstName{
				FirstName: importedCustomer.FirstName,
			},
		}

		owners, exists, err := p.FindAttributeOwners(owned)
		if err != nil {
			return err
		}

		if !exists {
			p.currentState.OwnedAttributes = append(p.currentState.OwnedAttributes, owned)
		}

		if p.Result.FirstName == "" || p.canSet(owners) {
			p.Result.FirstName = importedCustomer.FirstName
		}
	}

	return nil
}

func (p *Patcher) applyLastName(importedCustomer *customerv1.Customer) error {
	if importedCustomer.LastName != "" {
		owned := &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_LastName{
				LastName: importedCustomer.LastName,
			},
		}

		owners, exists, err := p.FindAttributeOwners(owned)
		if err != nil {
			return err
		}

		if !exists {
			p.currentState.OwnedAttributes = append(p.currentState.OwnedAttributes, owned)
		}

		if (p.Result.LastName == "" && len(owners) == 0) || p.canSet(owners) {
			p.Result.LastName = importedCustomer.LastName
		}
	}

	return nil
}

func (p *Patcher) applyAddressList(importedCustomer *customerv1.Customer) error {
	for _, value := range importedCustomer.Addresses {
		owned := &customerv1.OwnedAttribute{
			Kind: &customerv1.OwnedAttribute_Address{
				Address: value,
			},
		}

		owners, exists, err := p.FindAttributeOwners(owned)
		if err != nil {
			return fmt.Errorf("%q: %w", value, err)
		}

		if !exists {
			p.currentState.OwnedAttributes = append(p.currentState.OwnedAttributes, owned)
		}

		if len(owners) == 0 {
			p.Result.Addresses = append(p.Result.Addresses, repo.Clone(value))
		}
	}

	return nil
}

func (p *Patcher) applyStringList(resultList *[]string, importedList []string, factory func(value string) *customerv1.OwnedAttribute) error {
	for _, value := range importedList {
		owned := factory(value)

		owners, exists, err := p.FindAttributeOwners(owned)
		if err != nil {
			return fmt.Errorf("%q: %w", value, err)
		}

		if !exists {
			p.currentState.OwnedAttributes = append(p.currentState.OwnedAttributes, owned)
		}

		if len(owners) == 0 {
			*resultList = append(*resultList, value)
		}
	}

	return nil
}

func findImporterState(importer, ref string, states []*customerv1.ImportState) ([]*customerv1.ImportState, *customerv1.ImportState) {
	var state *customerv1.ImportState

	for _, s := range states {
		if s.Importer == importer && s.InternalReference == ref {
			state = s
			break
		}
	}

	if state == nil {
		state = &customerv1.ImportState{
			Importer:          importer,
			InternalReference: ref,
		}

		states = append(states, state)
	}

	return states, state
}

func (p *Patcher) FindAttributeOwners(owned *customerv1.OwnedAttribute) ([]string, bool, error) {
	var (
		owners []string
		exists bool
	)

	for _, state := range p.States {
		for _, attr := range state.OwnedAttributes {
			if proto.Equal(attr, owned) {
				if state.Importer == p.Importer && state.InternalReference == p.Ref {
					exists = true
				}

				owners = append(owners, state.Importer)
			}
		}
	}

	return owners, exists, nil
}
