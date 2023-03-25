package models

import (
	"time"
)

type Organization struct {
	ID        string     `json:"id" gorm:"type:primary_key;"` //
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty" gorm:"index"`

	OrganizationType string   `json:"organization_type"`
	Name             string   `json:"name"`
	Taxonomy         []string `json:"taxonomy" gorm:"type:text;serializer:json"` // Taxonomy code mapping: http://www.wpc-edi.com/reference/codelists/healthcare/health-care-provider-taxonomy-code-set/
	IsSoleProprietor bool     `json:"is_sole_proprietor"`
	RelatedUrls      []string `json:"related_urls" gorm:"type:text;serializer:json"`
	//Source Updated At?

	Locations               []Location               `json:"-"`
	Endpoints               []Endpoint               `json:"-"`
	OrganizationIdentifiers []OrganizationIdentifier `json:"-"`
}
