package models

import (
	"github.com/fastenhealth/fasten-sources-etl/pkg/utils"
	"golang.org/x/exp/slices"
	"log"
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

	Locations               []Location               `json:"-" gorm:"many2many:org_locations;"`
	Endpoints               []Endpoint               `json:"-"`
	OrganizationIdentifiers []OrganizationIdentifier `json:"-"`
}

//OrgA must be the "found"/"existing" organization (with an Id)
func (orgA *Organization) MergeHasChanges(orgB *Organization) (hasChanges bool) {
	hasChanges = false

	orgAId, err := utils.NormalizeOrganizationId(orgA.Name)
	if err != nil {
		log.Printf("Error normalizing organization name: %s", err)
		return hasChanges
	}

	orgBId, err := utils.NormalizeOrganizationId(orgB.Name)
	if err != nil {
		log.Printf("Error normalizing organization name: %s", err)
		return hasChanges
	}

	if orgAId != orgBId {
		hasChanges = true
		log.Printf("found new organization name, adding as alias")
		//add a new organization name (alias)
		orgA.OrganizationIdentifiers = append(orgA.OrganizationIdentifiers, OrganizationIdentifier{
			IdentifierValue:   orgBId,
			IdentifierDisplay: orgB.Name,
			IdentifierType:    OrganizationIdentifierTypeNPI,
		})

	}
	if orgA.OrganizationType == "" && orgA.OrganizationType != orgB.OrganizationType {
		hasChanges = true
		log.Printf("setting organization type, existing is empty string.")
		orgA.OrganizationType = orgB.OrganizationType
	}

	slices.Sort(orgA.Taxonomy)
	slices.Sort(orgB.Taxonomy)
	if slices.Compare(orgA.Taxonomy, orgB.Taxonomy) != 0 {
		hasChanges = true
		log.Printf("taxonomy differencies, merging")

		taxonomyList := append(orgA.Taxonomy, orgB.Taxonomy...)
		slices.Sort(taxonomyList)
		taxonomyList = slices.Compact(taxonomyList)
		orgA.Taxonomy = taxonomyList
	}

	slices.Sort(orgA.RelatedUrls)
	slices.Sort(orgB.RelatedUrls)
	if slices.Compare(orgA.RelatedUrls, orgB.RelatedUrls) != 0 {
		hasChanges = true
		log.Printf("related urls are different, merging")

		relatedUrlsList := append(orgA.RelatedUrls, orgB.RelatedUrls...)
		slices.Sort(relatedUrlsList)
		relatedUrlsList = slices.Compact(relatedUrlsList)
		orgA.RelatedUrls = relatedUrlsList
	}

	if orgA.MergeLocationsHasChanges(orgB) {
		hasChanges = true
	}
	if orgA.MergeEndpointsHasChanges(orgB) {
		hasChanges = true
	}
	if orgA.MergeOrganizationIdentifiersHasChanges(orgB) {
		hasChanges = true
	}

	return hasChanges
}

func (orgA *Organization) MergeLocationsHasChanges(orgB *Organization) (hasChanges bool) {
	hasChanges = false

	//locB is the new location
	for _, locB := range orgB.Locations {
		found := false
		for _, locA := range orgA.Locations {
			if locA.Equal(&locB) {
				found = true
				break
			}
		}
		if !found {
			hasChanges = true
			log.Printf("found new location, adding: %v", locB)
			log.Printf("existing locations: %v", orgA.Locations)

			orgA.Locations = append(orgA.Locations, locB)
		}
	}
	return hasChanges
}

func (orgA *Organization) MergeEndpointsHasChanges(orgB *Organization) (hasChanges bool) {
	hasChanges = false

	for _, endB := range orgB.Endpoints {
		found := false
		for _, endA := range orgA.Endpoints {
			if endA.Equal(&endB) {
				found = true
				break
			}
		}
		if !found {
			hasChanges = true
			log.Printf("found new endpoint, adding: %v", endB)

			orgA.Endpoints = append(orgA.Endpoints, endB)
		}
	}

	return hasChanges
}

func (orgA *Organization) MergeOrganizationIdentifiersHasChanges(orgB *Organization) bool {
	hasChanges := false

	for _, idB := range orgB.OrganizationIdentifiers {
		found := false
		for _, idA := range orgA.OrganizationIdentifiers {
			if idA.Equal(&idB) {
				found = true
				break
			}
		}
		if !found {
			hasChanges = true
			log.Printf("found new orgid, adding: %v", idB)

			orgA.OrganizationIdentifiers = append(orgA.OrganizationIdentifiers, idB)
		}
	}

	return hasChanges
}
