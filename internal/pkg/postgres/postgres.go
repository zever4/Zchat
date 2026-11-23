package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"

	"github.com/pressly/goose"
)

func InitPostgresDB(ctx context.Context) (*sql.DB, error) {
	db, connector, err := open(getConnStr())
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	// Goose v2 has no context API. Calls have a default operation limit;
	// closing transports bounds the entire startup by the caller's context.
	stop := watch(ctx, connector.close)
	defer stop()
	if err = db.PingContext(ctx); err == nil {
		err = goose.SetDialect("postgres")
	}
	if err == nil {
		err = goose.Up(db, "./migrations")
	}
	stop()
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		Close(db)
		return nil, fmt.Errorf("initialize database: %w", err)
	}
	return db, nil
}

func getConnStr() string {
	value := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}
	u := &url.URL{Scheme: "postgres", Host: net.JoinHostPort(value("DB_HOST", "localhost"), value("DB_PORT", "5432")), Path: "/" + os.Getenv("DB_NAME")}
	u.User = url.UserPassword(os.Getenv("DB_USER"), os.Getenv("DB_PASS"))
	query := url.Values{"sslmode": []string{value("DB_SSLMODE", "disable")}}
	u.RawQuery = query.Encode()
	return u.String()
}
