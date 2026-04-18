package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func RunMigrations(databaseURL, migrationsDir string, logger *slog.Logger) error {
	sourceURL := migrationsSourceURL(migrationsDir)
	m, err := migrate.New(sourceURL, databaseURL)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}

	defer func() {
		sourceErr, dbErr := m.Close()
		if sourceErr != nil {
			logger.Warn("failed closing migration source", "error", sourceErr)
		}
		if dbErr != nil {
			logger.Warn("failed closing migration database driver", "error", dbErr)
		}
	}()

	err = m.Up()
	if errors.Is(err, migrate.ErrNoChange) {
		logger.Info("no new migrations to apply")
		return nil
	}
	if err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	logger.Info("migrations applied successfully")
	return nil
}

func migrationsSourceURL(dir string) string {
	normalized := strings.TrimSpace(dir)
	if normalized == "" {
		normalized = "migrations"
	}

	if strings.HasPrefix(normalized, "file://") {
		return normalized
	}

	normalized = strings.ReplaceAll(normalized, "\\", "/")
	return "file://" + normalized
}
