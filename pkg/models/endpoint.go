package models

import (
	"github.com/fastenhealth/fasten-sources-etl/pkg/utils"
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

func (endA *Endpoint) Equal(endB *Endpoint) bool {
	if utils.NormalizeEndpointId(endA.URL) != utils.NormalizeEndpointId(endB.URL) {
		return false
	}
	if endA.SourceUrl != endB.SourceUrl {
		return false
	}
	if endA.PlatformType != endB.PlatformType {
		return false
	}
	return true
}
