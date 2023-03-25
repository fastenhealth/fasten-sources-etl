package models

import (
	"time"
)

type Endpoint struct {
	ID             string     `json:"id" gorm:"primary_key;"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	DeletedAt      *time.Time `json:"deleted_at,omitempty" gorm:"index"`
	OrganizationID string     `json:"organization_id"` //foreign key

	URL          string `json:"url" gorm:"unique"` //guaranteed to have https/http scheme and '/' suffix
	SourceUrl    string `json:"source_url"`
	PlatformType string `json:"platform_type"`
}
