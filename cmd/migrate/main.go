// Command migrate brings the bucket and every database up to date, then exits.
// Run it on deploy where the server skips its boot migrations
// (RUN_MIGRATIONS=false, the default on Lambda), with the server's environment:
//
//	go run ./cmd/migrate
package main

import (
	"context"
	"log"
	"xprem/config"
	"xprem/internal/bucketmigration"
	"xprem/internal/database"
	"xprem/internal/database/clickhouse"
	"xprem/internal/database/postgres"
	"xprem/internal/database/postgres/migrations"
	"xprem/internal/jobs"

	_ "xprem/internal/bucketmigrations"
)

func main() {
	config.LoadConfig()
	ctx := context.Background()

	if err := bucketmigration.EnsureMigrations(); err != nil {
		log.Fatalf("🚨 [BUCKET] %v", err)
	}

	dbUrl := config.GetDBURL()
	if dbUrl == "" {
		log.Println("✅ No DB_URL (stateless mode): only the bucket needed migrating")
		return
	}
	if !database.IsValidDBURL(dbUrl) {
		log.Fatalf("Invalid database URL: %s", dbUrl)
	}
	dbEngine, err := database.NewPostgresEngine(ctx, database.LoadDBConfigFromEnv())
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer dbEngine.Close()
	// The Go migrations (backfills) query through the engine.
	migrations.SetEngine(dbEngine)
	postgres.RunDBMigrations(dbUrl)

	jobsClient, err := jobs.NewClient(dbEngine)
	if err != nil {
		log.Fatalf("Job system initialization failed: %v", err)
	}
	if err := jobsClient.Migrate(ctx); err != nil {
		log.Fatalf("Job system migration failed: %v", err)
	}

	// Mirrors the server: no ClickHouse connection under DISABLE_DEVICE_TELEMETRY.
	if chUrl := config.GetClickHouseURL(); chUrl != "" && !config.IsDeviceTelemetryDisabled() {
		clickhouse.RunDBMigrations(chUrl, dbUrl)
	}
	log.Println("✅ All migrations applied")
}
