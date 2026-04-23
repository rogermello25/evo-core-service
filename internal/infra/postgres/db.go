package postgres

import (
	"database/sql"
	"errors"
	"evo-ai-core-service/internal/config"
	"fmt"
	"log"
	"strconv"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	_ "github.com/lib/pq"
)

// ErrDBConnect is returned when all retry attempts to connect to the database fail.
var ErrDBConnect = errors.New("failed to connect to database after retries")

const maxRetryAttempts = 5

func buildDSN(DBConfig *config.DBConfig) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		DBConfig.Host, DBConfig.Port, DBConfig.User, DBConfig.Password, DBConfig.DBName, DBConfig.SSLMode)
}

// retryWithBackoff calls fn up to maxRetryAttempts times with exponential backoff (2s, 4s, 8s, 16s, 32s).
// Returns the last error if all attempts fail.
func retryWithBackoff(fn func() error) error {
	delay := 2 * time.Second
	var lastErr error
	for attempt := 1; attempt <= maxRetryAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if attempt < maxRetryAttempts {
			log.Printf("DB connect attempt %d/%d failed: %v — retrying in %s...",
				attempt, maxRetryAttempts, lastErr, delay)
			time.Sleep(delay)
			delay *= 2
		}
	}
	return lastErr
}

func Connect(DBConfig *config.DBConfig) (*sql.DB, error) {
	dsn := buildDSN(DBConfig)

	var conn *sql.DB
	err := retryWithBackoff(func() error {
		var openErr error
		conn, openErr = sql.Open("postgres", dsn)
		if openErr != nil {
			return openErr
		}
		if pingErr := conn.Ping(); pingErr != nil {
			conn.Close()
			return pingErr
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDBConnect, err)
	}

	log.Println("Connected to Postgres database.")
	return conn, nil
}

func ConnectGorm(DBConfig *config.DBConfig) (*gorm.DB, error) {
	dsn := buildDSN(DBConfig)

	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
		NowFunc: func() time.Time {
			return time.Now().UTC()
		},
		PrepareStmt:                              true,
		DisableForeignKeyConstraintWhenMigrating: false,
	}

	var conn *gorm.DB
	err := retryWithBackoff(func() error {
		var openErr error
		conn, openErr = gorm.Open(postgres.Open(dsn), gormConfig)
		if openErr != nil {
			return openErr
		}
		sqlDB, sqlErr := conn.DB()
		if sqlErr != nil {
			return sqlErr
		}
		return sqlDB.Ping()
	})

	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDBConnect, err)
	}

	sqlDB, err := conn.DB()
	if err != nil {
		return nil, fmt.Errorf("error getting underlying sql.DB: %w", err)
	}

	maxIdleConns := parseIntOrDefault(DBConfig.MaxIdleConns, 10)
	maxOpenConns := parseIntOrDefault(DBConfig.MaxOpenConns, 100)
	connMaxLifetime := parseDurationOrDefault(DBConfig.ConnMaxLifetime, time.Hour)
	connMaxIdleTime := parseDurationOrDefault(DBConfig.ConnMaxIdleTime, 30*time.Minute)

	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetConnMaxLifetime(connMaxLifetime)
	sqlDB.SetConnMaxIdleTime(connMaxIdleTime)

	log.Printf("Connected to Postgres database with connection pool: idle=%d, max=%d, lifetime=%s, idle_time=%s",
		maxIdleConns, maxOpenConns, connMaxLifetime, connMaxIdleTime)

	return conn, nil
}

func parseIntOrDefault(value string, defaultValue int) int {
	if value == "" {
		return defaultValue
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("Warning: Invalid integer value: %s, using default: %d", value, defaultValue)
		return defaultValue
	}
	return intValue
}

func parseDurationOrDefault(value string, defaultValue time.Duration) time.Duration {
	if value == "" {
		return defaultValue
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("Warning: Invalid duration value: %s, using default: %s", value, defaultValue.String())
		return defaultValue
	}
	return duration
}
