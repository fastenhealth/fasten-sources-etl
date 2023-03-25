package models

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
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
	ID             uuid.UUID     `json:"id" gorm:"type:uuid;primary_key;"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
	DeletedAt      *time.Time    `json:"deleted_at,omitempty" gorm:"index"`
	OrganizationID string        `json:"organization_id"` //foreign key
	Organization   *Organization `json:"-"`

	IdentifierType    OrganizationIdentifierType `json:"identifier_type" gorm:"uniqueIndex:idx_org_identifier_type_value"`
	IdentifierValue   string                     `json:"identifier_value" gorm:"uniqueIndex:idx_org_identifier_type_value"`
	IdentifierDisplay string                     `json:"identifier_display"`
}

func (oi *OrganizationIdentifier) BeforeCreate(tx *gorm.DB) error {
	oi.ID = uuid.New()
	return nil
}
