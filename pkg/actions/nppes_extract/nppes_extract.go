package main

import (
	"encoding/csv"
	"github.com/fastenhealth/fasten-sources-etl/pkg/database"
	"github.com/fastenhealth/fasten-sources-etl/pkg/models"
	"github.com/fastenhealth/fasten-sources-etl/pkg/utils"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type NPPESColumnType int

const (
	NPPESColumnTypeNPI                NPPESColumnType = 0 //"NPI"
	NPPESColumnTypeEntityTypeCode     NPPESColumnType = 1 //"Entity Type Code"
	NPPESColumnTypeEIN                NPPESColumnType = 3 //"Employer Identification Number (EIN)"
	NPPESColumnTypeOrganizationName   NPPESColumnType = 4 //"Provider Organization Name (Legal Business Name)"
	NPPESColumnTypeProviderLastName   NPPESColumnType = 5 //"Provider Last Name (Legal Name)"
	NPPESColumnTypeProviderFirstName  NPPESColumnType = 6 //"Provider First Name"
	NPPESColumnTypeProviderMiddleName NPPESColumnType = 7 //"Provider Middle Name"
	NPPESColumnTypeProviderNamePrefix NPPESColumnType = 8 //"Provider Name Prefix Text"
	NPPESColumnTypeProviderNameSuffix NPPESColumnType = 9 //"Provider Name Suffix Text"

	NPPESColumnTypeProviderOtherOrganizationName NPPESColumnType = 11 //"Provider Other Organization Name"
	NPPESColumnTypeProviderOtherLastName         NPPESColumnType = 13 //"Provider Other Last Name"
	NPPESColumnTypeProviderOtherFirstName        NPPESColumnType = 14 //"Provider Other First Name"
	NPPESColumnTypeProviderOtherMiddleName       NPPESColumnType = 15 //"Provider Other Middle Name"
	NPPESColumnTypeProviderOtherNamePrefix       NPPESColumnType = 16 //"Provider Other Name Prefix Text"
	NPPESColumnTypeProviderOtherNameSuffix       NPPESColumnType = 17 //"Provider Other Name Suffix Text"

	NPPESColumnTypeProviderFirstLineBusinessPracticeLocationAddress       NPPESColumnType = 28 //"Provider First Line Business Practice Location Address"
	NPPESColumnTypeProviderSecondLineBusinessPracticeLocationAddress      NPPESColumnType = 29 //"Provider Second Line Business Practice Location Address"
	NPPESColumnTypeProviderBusinessPracticeLocationAddressCityName        NPPESColumnType = 30 //"Provider Business Practice Location Address City Name"
	NPPESColumnTypeProviderBusinessPracticeLocationAddressStateName       NPPESColumnType = 31 //"Provider Business Practice Location Address State Name"
	NPPESColumnTypeProviderBusinessPracticeLocationAddressPostalCode      NPPESColumnType = 32 //"Provider Business Practice Location Address Postal Code"
	NPPESColumnTypeProviderBusinessPracticeLocationAddressCountryCode     NPPESColumnType = 33 //"Provider Business Practice Location Address Country Code"
	NPPESColumnTypeProviderBusinessPracticeLocationAddressTelephoneNumber NPPESColumnType = 34 //"Provider Business Practice Location Address Telephone Number"

	NPPESColumTypeLastUpdateDate            NPPESColumnType = 37 //"Last Update Date"
	NPPESColumTypeNPIDeactivationReasonCode NPPESColumnType = 38 //"NPI Deactivation Reason Code"

	NPPESColumTypeHealthcareProviderTaxonomyCode_1  NPPESColumnType = 47  //"Healthcare Provider Taxonomy Code_1"
	NPPESColumTypeHealthcareProviderTaxonomyCode_2  NPPESColumnType = 51  //"Healthcare Provider Taxonomy Code_2"
	NPPESColumTypeHealthcareProviderTaxonomyCode_3  NPPESColumnType = 55  //"Healthcare Provider Taxonomy Code_3"
	NPPESColumTypeHealthcareProviderTaxonomyCode_4  NPPESColumnType = 59  //"Healthcare Provider Taxonomy Code_4"
	NPPESColumTypeHealthcareProviderTaxonomyCode_5  NPPESColumnType = 63  //"Healthcare Provider Taxonomy Code_5"
	NPPESColumTypeHealthcareProviderTaxonomyCode_6  NPPESColumnType = 67  //"Healthcare Provider Taxonomy Code_6"
	NPPESColumTypeHealthcareProviderTaxonomyCode_7  NPPESColumnType = 71  //"Healthcare Provider Taxonomy Code_7"
	NPPESColumTypeHealthcareProviderTaxonomyCode_8  NPPESColumnType = 75  //"Healthcare Provider Taxonomy Code_8"
	NPPESColumTypeHealthcareProviderTaxonomyCode_9  NPPESColumnType = 79  //"Healthcare Provider Taxonomy Code_9"
	NPPESColumTypeHealthcareProviderTaxonomyCode_10 NPPESColumnType = 83  //"Healthcare Provider Taxonomy Code_10"
	NPPESColumTypeHealthcareProviderTaxonomyCode_11 NPPESColumnType = 87  //"Healthcare Provider Taxonomy Code_11"
	NPPESColumTypeHealthcareProviderTaxonomyCode_12 NPPESColumnType = 91  //"Healthcare Provider Taxonomy Code_12"
	NPPESColumTypeHealthcareProviderTaxonomyCode_13 NPPESColumnType = 95  //"Healthcare Provider Taxonomy Code_13"
	NPPESColumTypeHealthcareProviderTaxonomyCode_14 NPPESColumnType = 99  //"Healthcare Provider Taxonomy Code_14"
	NPPESColumTypeHealthcareProviderTaxonomyCode_15 NPPESColumnType = 103 //"Healthcare Provider Taxonomy Code_15"

	NPPESColumTypeIsSoleProprietor      NPPESColumnType = 307 //"Is Sole Proprietor"
	NPPESColumTypeIsOrganizationSubpart NPPESColumnType = 308 //"Is Organization Subpart"
	NPPESColumTypeParentOrganizationLBN NPPESColumnType = 309 //"Parent Organization LBN"
	NPPESColumTypeParentOrganizationTIN NPPESColumnType = 310 //"Parent Organization TIN"

	NPPESColumTypeHealthcareProviderTaxonomyGroup_1  NPPESColumnType = 314 //"Healthcare Provider Taxonomy Group_1"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_2  NPPESColumnType = 315 //"Healthcare Provider Taxonomy Group_2"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_3  NPPESColumnType = 316 //"Healthcare Provider Taxonomy Group_3"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_4  NPPESColumnType = 317 //"Healthcare Provider Taxonomy Group_4"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_5  NPPESColumnType = 318 //"Healthcare Provider Taxonomy Group_5"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_6  NPPESColumnType = 319 //"Healthcare Provider Taxonomy Group_6"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_7  NPPESColumnType = 320 //"Healthcare Provider Taxonomy Group_7"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_8  NPPESColumnType = 321 //"Healthcare Provider Taxonomy Group_8"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_9  NPPESColumnType = 322 //"Healthcare Provider Taxonomy Group_9"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_10 NPPESColumnType = 323 //"Healthcare Provider Taxonomy Group_10"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_11 NPPESColumnType = 324 //"Healthcare Provider Taxonomy Group_11"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_12 NPPESColumnType = 325 //"Healthcare Provider Taxonomy Group_12"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_13 NPPESColumnType = 326 //"Healthcare Provider Taxonomy Group_13"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_14 NPPESColumnType = 327 //"Healthcare Provider Taxonomy Group_14"
	NPPESColumTypeHealthcareProviderTaxonomyGroup_15 NPPESColumnType = 328 //"Healthcare Provider Taxonomy Group_15"

)

func main() {
	// setup reader
	csvIn, err := os.Open("/Users/jason/Downloads/NPPES_Data_Dissemination_September_2022/npidata_pfile_20050523-20220911-1000.csv")
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(csvIn)

	// setup database
	nppesDatabase, err := database.NewRepository(logrus.New())
	if err != nil {
		log.Fatal("Unable to open/load database")
	}
	//w := csv.NewWriter(csvOut)
	//defer csvOut.Close()

	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		//start processing entry. PSEUDOCODE:
		//1. filter all entries that are deactivated (index 38, "NPI Deactivation Reason Code")
		if rec[NPPESColumTypeNPIDeactivationReasonCode] != "" {
			continue
		}
		//2. filter if missing entity type code (index 1, "Entity Type Code")
		if rec[NPPESColumnTypeEntityTypeCode] == "" {
			continue
		}
		////3. filter if not organization (index 1, "Entity Type Code") or individual (index 1, "Entity Type Code") with sole proprietor (index 307, "Is Sole Proprietor")
		//if rec[1] != "1" && (rec[1] == "2" && rec[307] != "Y") {
		//	continue
		//}
		//4. filter if missing organziation name (index 4, "Provider Organization Name (Legal Business Name)" or individual last name(index 5, "Provider Last Name (Legal Name)")
		if rec[NPPESColumnTypeOrganizationName] == "" && rec[NPPESColumnTypeProviderLastName] == "" {
			continue
		}

		//5. first pass, skip if Organization Subpart (index 308, "Is Organization Subpart")
		if rec[NPPESColumTypeIsOrganizationSubpart] == "Y" {
			continue
		}

		var name string
		var alias string
		if rec[NPPESColumnTypeEntityTypeCode] == "2" { //organization
			name = rec[NPPESColumnTypeOrganizationName]
			alias = rec[NPPESColumnTypeProviderOtherOrganizationName]
		} else {
			parts := []string{
				rec[NPPESColumnTypeProviderNamePrefix],
				rec[NPPESColumnTypeProviderFirstName],
				rec[NPPESColumnTypeProviderMiddleName],
				rec[NPPESColumnTypeProviderLastName],
				rec[NPPESColumnTypeProviderNameSuffix],
			}
			parts = deleteEmpty(parts)
			name = strings.Join(parts, " ")

			aliasParts := []string{
				rec[NPPESColumnTypeProviderOtherNamePrefix],
				rec[NPPESColumnTypeProviderOtherFirstName],
				rec[NPPESColumnTypeProviderOtherMiddleName],
				rec[NPPESColumnTypeProviderOtherLastName],
				rec[NPPESColumnTypeProviderOtherNameSuffix],
			}
			aliasParts = deleteEmpty(aliasParts)
			if len(aliasParts) > 1 {
				alias = strings.Join(parts, " ")
			}
		}

		log.Printf("Is Organization Subpart: %s", rec[NPPESColumTypeIsOrganizationSubpart])
		log.Printf("Parent Organization LBN: %s", rec[NPPESColumTypeParentOrganizationLBN])
		log.Printf("Parent Organization TIN: %s", rec[NPPESColumTypeParentOrganizationTIN])

		orgId, err := utils.NormalizeOrganizationId(name)
		if err != nil {
			log.Fatal(err)
			return
		}

		identifiers := []models.OrganizationIdentifier{
			models.OrganizationIdentifier{
				IdentifierValue:   orgId,
				IdentifierType:    models.OrganizationIdentifierTypeName,
				IdentifierDisplay: name,
			},
		}
		if len(alias) > 0 {
			aliasId, err := utils.NormalizeOrganizationId(alias)
			if err != nil {
				log.Fatal(err)
				return
			}
			identifiers = append(identifiers, models.OrganizationIdentifier{
				IdentifierValue:   aliasId,
				IdentifierType:    models.OrganizationIdentifierTypeName,
				IdentifierDisplay: alias,
			})
		}

		if len(rec[NPPESColumnTypeNPI]) > 0 {
			if rec[NPPESColumTypeIsOrganizationSubpart] == "Y" {
				identifiers = append(identifiers, models.OrganizationIdentifier{
					IdentifierValue: rec[NPPESColumnTypeNPI],
					IdentifierType:  models.OrganizationIdentifierTypeNPI,
				})
			} else {
				//add as primary and secondary NPI
				identifiers = append(identifiers, models.OrganizationIdentifier{
					IdentifierValue: rec[NPPESColumnTypeNPI],
					IdentifierType:  models.OrganizationIdentifierTypePrimaryNPI,
				})
				identifiers = append(identifiers, models.OrganizationIdentifier{
					IdentifierValue: rec[NPPESColumnTypeNPI],
					IdentifierType:  models.OrganizationIdentifierTypeNPI,
				})
			}
		}

		if len(rec[NPPESColumnTypeEIN]) > 0 {
			identifiers = append(identifiers, models.OrganizationIdentifier{
				IdentifierValue: rec[NPPESColumnTypeEIN],
				IdentifierType:  models.OrganizationIdentifierTypeEIN,
			})
		}

		address := models.Location{
			Line: deleteEmpty([]string{
				rec[NPPESColumnTypeProviderFirstLineBusinessPracticeLocationAddress],
				rec[NPPESColumnTypeProviderSecondLineBusinessPracticeLocationAddress],
			}),
			City:       rec[NPPESColumnTypeProviderBusinessPracticeLocationAddressCityName],
			State:      rec[NPPESColumnTypeProviderBusinessPracticeLocationAddressStateName],
			PostalCode: rec[NPPESColumnTypeProviderBusinessPracticeLocationAddressPostalCode],
			Country:    rec[NPPESColumnTypeProviderBusinessPracticeLocationAddressCountryCode],
		}

		org := models.Organization{
			ID:               orgId,
			OrganizationType: rec[1],
			Name:             name,
			//Addresses:                    []string{},
			CreatedAt:        time.Now(),
			Taxonomy:         taxonomyCodes(rec),
			IsSoleProprietor: rec[NPPESColumTypeIsSoleProprietor] == "Y",

			//Links
			OrganizationIdentifiers: identifiers,
			Locations:               []models.Location{address},
		}
		err = nppesDatabase.CreateOrganization(&org)
		log.Printf(" %v", err)
		log.Printf(" %v", rec)

	}
}

func taxonomyCodes(record []string) []string {
	taxonomyColumns := []NPPESColumnType{
		NPPESColumTypeHealthcareProviderTaxonomyCode_1,
		NPPESColumTypeHealthcareProviderTaxonomyCode_2,
		NPPESColumTypeHealthcareProviderTaxonomyCode_3,
		NPPESColumTypeHealthcareProviderTaxonomyCode_4,
		NPPESColumTypeHealthcareProviderTaxonomyCode_5,
		NPPESColumTypeHealthcareProviderTaxonomyCode_6,
		NPPESColumTypeHealthcareProviderTaxonomyCode_7,
		NPPESColumTypeHealthcareProviderTaxonomyCode_8,
		NPPESColumTypeHealthcareProviderTaxonomyCode_9,
		NPPESColumTypeHealthcareProviderTaxonomyCode_10,
		NPPESColumTypeHealthcareProviderTaxonomyCode_11,
		NPPESColumTypeHealthcareProviderTaxonomyCode_12,
		NPPESColumTypeHealthcareProviderTaxonomyCode_13,
		NPPESColumTypeHealthcareProviderTaxonomyCode_14,
		NPPESColumTypeHealthcareProviderTaxonomyCode_15,

		NPPESColumTypeHealthcareProviderTaxonomyGroup_1,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_2,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_3,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_4,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_5,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_6,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_7,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_8,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_9,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_10,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_11,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_12,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_13,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_14,
		NPPESColumTypeHealthcareProviderTaxonomyGroup_15,
	}

	var codes []string
	for _, colCode := range taxonomyColumns {
		code := record[colCode]
		if code != "" {
			codes = append(codes, code)
		}
	}
	return codes
}

func deleteEmpty(s []string) []string {
	var r []string
	for _, str := range s {
		if str != "" {
			r = append(r, str)
		}
	}
	return r
}
