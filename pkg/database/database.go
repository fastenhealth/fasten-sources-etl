package database

import (
	"fmt"
	"github.com/fastenhealth/fasten-sources-etl/pkg/models"
	"github.com/glebarez/sqlite"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"log"
	"net/url"
	"strings"
)

func NewRepository(globalLogger logrus.FieldLogger) (*SqliteRepository, error) {
	//backgroundContext := context.Background()
	databaseLocation := "data/fasten-etl-database.db"

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Gorm/SQLite setup
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	globalLogger.Infof("Trying to connect to sqlite db: %s\n", databaseLocation)

	// When a transaction cannot lock the database, because it is already locked by another one,
	// SQLite by default throws an error: database is locked. This behavior is usually not appropriate when
	// concurrent access is needed, typically when multiple processes write to the same database.
	// PRAGMA busy_timeout lets you set a timeout or a handler for these events. When setting a timeout,
	// SQLite will try the transaction multiple times within this timeout.
	// fixes #341
	// https://rsqlite.r-dbi.org/reference/sqlitesetbusyhandler
	// retrying for 30000 milliseconds, 30seconds - this would be unreasonable for a distributed multi-tenant application,
	// but should be fine for local usage.
	pragmaStr := sqlitePragmaString(map[string]string{
		"busy_timeout": "30000",
		"foreign_keys": "ON",
	})
	database, err := gorm.Open(sqlite.Open(databaseLocation+pragmaStr), &gorm.Config{
		//TODO: figure out how to log database queries again.
		//Logger: Logger
		DisableForeignKeyConstraintWhenMigrating: true,
	})

	if strings.ToUpper("DEBUG") == "DEBUG" {
		database = database.Debug() //set debug globally
	}

	if err != nil {
		return nil, fmt.Errorf("Failed to connect to database! - %v", err)
	}
	globalLogger.Infof("Successfully connected to fasten sqlite db: %s\n", databaseLocation)

	deviceRepo := SqliteRepository{
		Logger:     globalLogger,
		GormClient: database,
	}

	//TODO: automigrate for now
	err = deviceRepo.Migrate()
	if err != nil {
		return nil, err
	}

	return &deviceRepo, nil
}

type SqliteRepository struct {
	Logger logrus.FieldLogger

	GormClient *gorm.DB
}

func (sr *SqliteRepository) Migrate() error {
	err := sr.GormClient.AutoMigrate(
		&models.Organization{},
		&models.Location{},
		&models.Endpoint{},
		&models.OrganizationIdentifier{},
	)
	if err != nil {
		return fmt.Errorf("Failed to automigrate! - %v", err)
	}
	return nil
}

func (sr *SqliteRepository) Close() error {
	return nil
}

func (sr *SqliteRepository) CreateOrganization(org *models.Organization) error {
	return sr.GormClient.Create(org).Error
}

func (sr *SqliteRepository) FindOrganizationById(orgId string) (*models.Organization, error) {
	var org models.Organization
	err := sr.GormClient.First(&org, "id = ?", orgId).Error
	if err != nil {
		return nil, err
	}
	return &org, nil
}

func (sr *SqliteRepository) FindOrganizationByIdentifiers(identifiers []models.OrganizationIdentifier) (*models.Organization, error) {

	var orgIdentifier models.OrganizationIdentifier
	for _, identifier := range identifiers {
		err := sr.GormClient.Preload("Organization").
			Preload("Organization.Locations").
			Preload("Organization.Endpoints").
			Preload("Organization.OrganizationIdentifiers").
			Where(models.OrganizationIdentifier{IdentifierType: identifier.IdentifierType, IdentifierValue: identifier.IdentifierValue}).
			First(&orgIdentifier).Error
		if err == nil {
			break
		}
	}

	if orgIdentifier.OrganizationID != "" {
		log.Printf("Found organization: %v", orgIdentifier.OrganizationID)
		return orgIdentifier.Organization, nil
	}

	return nil, fmt.Errorf("No organization found for identifiers: %v", identifiers)
}

func (sr *SqliteRepository) UpdateOrganization(org *models.Organization) error {
	return sr.GormClient.Updates(org).Error
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Utilities
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func sqlitePragmaString(pragmas map[string]string) string {
	q := url.Values{}
	for key, val := range pragmas {
		q.Add("_pragma", key+"="+val)
	}

	queryStr := q.Encode()
	if len(queryStr) > 0 {
		return "?" + queryStr
	}
	return ""
}
