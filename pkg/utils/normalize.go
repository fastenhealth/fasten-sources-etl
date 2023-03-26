package utils

import (
	"fmt"
	"github.com/Boostport/address"
	"regexp"
	"strings"
)

// Prepends url with https:// if needed
func NormalizeURL(url string) string {
	normalized := url
	// for cases such as foobar.com
	if !strings.HasPrefix(url, "https://") && !strings.HasPrefix(url, "http://") {
		normalized = "https://" + normalized
	}

	return normalized
}

// Prepends url with https:// and appends with metadata/ if needed
func NormalizeEndpointURL(url string) string {
	normalized := NormalizeURL(url)

	// for cases such as foobar.com/
	if !strings.HasSuffix(url, "/") {
		normalized = normalized + "/"
	}
	return normalized
}

func NormalizeEndpointId(url string) string {
	return strings.ToLower(NormalizeEndpointURL(url))
}

func NormalizeOrganizationId(orgName string) (string, error) {
	orgName = strings.ReplaceAll(orgName, "-", " ")
	orgName = strings.ReplaceAll(orgName, "/", " ")
	orgName = strings.ReplaceAll(orgName, ",", " ")
	// Regex for only letters
	reg, err := regexp.Compile(`[^a-zA-Z0-9\s]+`)
	if err != nil {
		return "", fmt.Errorf("error compiling regex for normalizing organization name: %v", err)
	}
	characterStrippedName := reg.ReplaceAllString(orgName, "")
	return strings.ToUpper(characterStrippedName), nil
}

func NormalizeLocationId(addrLines []string, addrCity string, addrState string, addrZip string, addrCountry string) (string, error) {

	addr, err := address.NewValid(
		address.WithStreetAddress(addrLines),
		address.WithLocality(addrCity),
		address.WithAdministrativeArea(addrState),
		address.WithPostCode(addrZip[:5]),
		address.WithCountry(addrCountry),
	)
	if err != nil {
		addrStr := fmt.Sprintf("address: %v, %s, %s, %s, %s", addrLines, addrCity, addrState, addrZip, addrCountry)

		return "", fmt.Errorf("error normalizing location(%s) id: %v", addrStr, err)
	}

	defStringFormatter := address.DefaultFormatter{
		Output: address.StringOutputter{},
	}

	locationId := strings.Join(strings.Split(defStringFormatter.Format(addr, "en"), "\n"), ",")

	return locationId, err
}
