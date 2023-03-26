package models

import (
	"github.com/fastenhealth/fasten-sources-etl/pkg/utils"
	"gorm.io/gorm"
	"time"
)

type Location struct {
	ID        string     `json:"id" gorm:"primary_key;"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty" gorm:"index"`

	Line       []string `json:"line" gorm:"type:text;serializer:json"` // the lines of the address. For example, "123 Governors Ln".
	City       string   `json:"city"`
	State      string   `json:"state"`       // the two-letter state or posession abbreviation as defined in https://pe.usps.com/text/pub28/28apb.htm.
	PostalCode string   `json:"postal_code"` // the five-digit zip code.
	Country    string   `json:"country"`     // the two-letter country code

	Organizations []Organization `json:"-" gorm:"many2many:org_locations;"`
}

func (oi *Location) BeforeCreate(tx *gorm.DB) error {
	locId, err := utils.NormalizeLocationId(oi.Line, oi.City, oi.State, oi.PostalCode, oi.Country)
	if err != nil {
		return err
	}
	oi.ID = locId
	return nil
}

func (locA *Location) Equal(locB *Location) bool {

	locAAddr, err := utils.NormalizeLocationId(locA.Line, locA.City, locA.State, locA.PostalCode, locA.Country)
	if err != nil {
		return false
	}
	locBAddr, err := utils.NormalizeLocationId(locB.Line, locB.City, locB.State, locB.PostalCode, locB.Country)
	if err != nil {
		return false
	}

	return locAAddr == locBAddr
}
