package models

import (
	"fmt"
	"time"

	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/ql"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var PatientSchema = ql.FieldList{
	ql.FieldSpec{
		Name:         "patientId",
		Aliases:      []string{"id"},
		TypeResolver: objectID,
	},
	ql.FieldSpec{
		Name:         "customerId",
		TypeResolver: objectID,
	},
	ql.FieldSpec{
		Name: "patientName",
	},
	ql.FieldSpec{
		Name: "species",
	},
	ql.FieldSpec{
		Name: "breed",
	},
	ql.FieldSpec{
		Name: "gender",
		TypeResolver: ql.TypeResolverFunc(func(s string) (any, error) {
			if s == "" || s == "null" || s == "nil" {
				return customerv1.PatientGender_PATIENT_GENDER_UNSPECIFIED, nil
			}

			g, ok := stringToGender[s]
			if !ok {
				return g, fmt.Errorf("invalid gender value %q", s)
			}

			return g, nil
		}),
	},
	ql.FieldSpec{
		Name:         "birthday",
		TypeResolver: ql.TimeStartKeywordType(time.Local),
	},
	ql.FieldSpec{
		Name: "comment",
	},
	ql.FieldSpec{
		Name:         "isAlive",
		TypeResolver: ql.BooleanType(),
	},
	ql.FieldSpec{
		Name: "chipNumber",
	},
	ql.FieldSpec{
		Name: "color",
	},
	ql.FieldSpec{
		Name: "additionalUniqueId",
	},
	ql.FieldSpec{
		Name: "internalReference",
	},
	ql.FieldSpec{
		Name: "importer",
	},
	ql.FieldSpec{
		Name:         "firstSeen",
		TypeResolver: ql.TimeStartKeywordType(time.Local),
	},
	ql.FieldSpec{
		Name:         "lastUpdated",
		TypeResolver: ql.TimeStartKeywordType(time.Local),
	},
}

var objectID ql.TypeResolverFunc = func(s string) (any, error) {
	return primitive.ObjectIDFromHex(s)
}

var stringToGender = map[string]customerv1.PatientGender{
	"m":  customerv1.PatientGender_PATIENT_GENDER_MALE,
	"mk": customerv1.PatientGender_PATIENT_GENDER_MALE_CASTRATED,
	"w":  customerv1.PatientGender_PATIENT_GENDER_FEMALE,
	"wk": customerv1.PatientGender_PATIENT_GENDER_FEMALE_CASTRATED,
}

var genderToString map[customerv1.PatientGender]string

func init() {
	genderToString = make(map[customerv1.PatientGender]string)
	for s, g := range stringToGender {
		genderToString[g] = s
	}
}
