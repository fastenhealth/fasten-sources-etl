package models

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

type Location struct {
	ID             uuid.UUID  `json:"id" gorm:"type:uuid;primary_key;"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	DeletedAt      *time.Time `json:"deleted_at,omitempty" gorm:"index"`
	OrganizationID string     `json:"organization_id"` //foreign key

	Line       []string `json:"line" gorm:"type:text;serializer:json"` // the lines of the address. For example, "123 Governors Ln".
	City       string   `json:"city"`
	State      string   `json:"state"`       // the two-letter state or posession abbreviation as defined in https://pe.usps.com/text/pub28/28apb.htm.
	PostalCode string   `json:"postal_code"` // the five-digit zip code.
	Country    string   `json:"country"`     // the two-letter country code
}

func (oi *Location) BeforeCreate(tx *gorm.DB) error {
	oi.ID = uuid.New()
	return nil
}
