package models

import (
	"time"
)

type OrganizationIdentifierType string

const (
	OrganizationIdentifierTypePrimaryNPI OrganizationIdentifierType = "OrganizationIdentifierTypePrimaryNPI"
	OrganizationIdentifierTypeNPI        OrganizationIdentifierType = "OrganizationIdentifierTypeNPI"
	OrganizationIdentifierTypeEIN        OrganizationIdentifierType = "OrganizationIdentifierTypeEIN"
	OrganizationIdentifierTypeName       OrganizationIdentifierType = "OrganizationIdentifierTypeName"
)

type OrganizationIdentifier struct {
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
	DeletedAt      *time.Time    `json:"deleted_at,omitempty" gorm:"index"`
	OrganizationID string        `json:"organization_id"` //foreign key
	Organization   *Organization `json:"-"`

	IdentifierType    OrganizationIdentifierType `json:"identifier_type" gorm:"primary_key"`
	IdentifierValue   string                     `json:"identifier_value" gorm:"primary_key"`
	IdentifierDisplay string                     `json:"identifier_display"`
}

func (oi *OrganizationIdentifier) Equal(oi2 *OrganizationIdentifier) bool {
	return oi.IdentifierType == oi2.IdentifierType && oi.IdentifierValue == oi2.IdentifierValue
}
