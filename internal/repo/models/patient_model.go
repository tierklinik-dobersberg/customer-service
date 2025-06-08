package models

import (
	"fmt"
	"time"

	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Patient struct {
	PatientId          primitive.ObjectID `bson:"_id"`
	CustomerId         primitive.ObjectID `bson:"customerId"`
	PatientName        string             `bson:"patientName"`
	Species            string             `bson:"species"`
	Breed              string             `bson:"breed"`
	Birthday           time.Time          `bson:"birthday"`
	Gender             string             `bson:"gender"`
	Comment            string             `bson:"comment"`
	IsAlive            bool               `bson:"isAlive"`
	ChipNumber         string             `bson:"chipNumber"`
	Color              string             `bson:"color"`
	ExtraData          map[string]any     `bson:"extraData"`
	AdditionalUniqueId string             `bson:"additionalUniqueId"`
	InternalReference  string             `bson:"internalReference"`
	Importer           string             `bson:"importer"`
	FirstSeen          time.Time          `bson:"firstSeen"`
	LastUpdated        time.Time          `bson:"lastUpdated"`
	AssignedSpecies    string             `bson:"assignedSpecies"`
}

func (p Patient) ToProto() (*customerv1.Patient, error) {
	pb := &customerv1.Patient{
		PatientId:           p.PatientId.Hex(),
		CustomerId:          p.CustomerId.Hex(),
		PatientName:         p.PatientName,
		Species:             p.Species,
		Breed:               p.Breed,
		Gender:              stringToGender[p.Gender],
		Comment:             p.Comment,
		ChipNumber:          p.ChipNumber,
		Color:               p.Color,
		AdditionUniqueId:    p.AdditionalUniqueId,
		InternalReference:   p.InternalReference,
		Importer:            p.Importer,
		LastUpdated:         timestamppb.New(p.LastUpdated),
		FirstSeen:           timestamppb.New(p.FirstSeen),
		IsAlive:             p.IsAlive,
		AssignedSpeciesName: p.AssignedSpecies,
	}

	if !p.Birthday.IsZero() {
		pb.Birthday = commonv1.FromTime(p.Birthday)
	}

	if p.ExtraData != nil {
		val, err := structpb.NewStruct(p.ExtraData)
		if err != nil {
			return pb, err
		}

		pb.ExtraData = val
	}

	return pb, nil
}

func PatientFromProto(pb *customerv1.Patient) (Patient, error) {
	p := Patient{
		PatientName:        pb.PatientName,
		Species:            pb.Species,
		Breed:              pb.Breed,
		Gender:             genderToString[pb.Gender],
		Comment:            pb.Comment,
		Color:              pb.Color,
		ChipNumber:         pb.ChipNumber,
		AdditionalUniqueId: pb.AdditionUniqueId,
		InternalReference:  pb.InternalReference,
		Importer:           pb.Importer,
		FirstSeen:          pb.FirstSeen.AsTime(),
		LastUpdated:        pb.LastUpdated.AsTime(),
		IsAlive:            pb.IsAlive,
		AssignedSpecies:    pb.AssignedSpeciesName,
	}

	if pb.ExtraData != nil {
		p.ExtraData = pb.ExtraData.AsMap()
	}

	if pb.Birthday != nil {
		p.Birthday = pb.Birthday.AsTimeInLocation(time.Local)
	}

	if pb.PatientId != "" {
		patientId, err := primitive.ObjectIDFromHex(pb.PatientId)
		if err != nil {
			return p, fmt.Errorf("invalid patient id: %w", err)
		}

		p.PatientId = patientId
	}

	if pb.CustomerId != "" {
		customerId, err := primitive.ObjectIDFromHex((pb.CustomerId))
		if err != nil {
			return p, fmt.Errorf("invalid customer id: %w", err)
		}
		p.CustomerId = customerId
	}

	return p, nil
}

type Anamnesis struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Order     int64              `bson:"order"`
	PatientID primitive.ObjectID `bson:"patientId"`
	CreatedAt time.Time          `bson:"createdAt,omitempty"`
	Text      string             `bson:"text"`
	Diagnosis string             `bson:"diagnosis"`
}

func (a Anamnesis) ToProto() *customerv1.Anamnesis {
	return &customerv1.Anamnesis{
		Time:      timestamppb.New(a.CreatedAt),
		Text:      a.Text,
		Diagnosis: a.Diagnosis,
		Order:     a.Order,
	}
}
