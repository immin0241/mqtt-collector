package workers

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestClassifyDBError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want ErrorKind
	}{
		{"data exception", &pgconn.PgError{Code: "22P02"}, KindPermanent},
		{"numeric out of range", &pgconn.PgError{Code: "22003"}, KindPermanent},
		{"foreign key", &pgconn.PgError{Code: "23503"}, KindPermanent},
		{"check", &pgconn.PgError{Code: "23514"}, KindPermanent},
		{"wrapped permanent", fmt.Errorf("gorm: %w", &pgconn.PgError{Code: "23502"}), KindPermanent},
		{"connection exception", &pgconn.PgError{Code: "08006"}, KindTransient},
		{"admin shutdown", &pgconn.PgError{Code: "57P01"}, KindTransient},
		{"serialization failure", &pgconn.PgError{Code: "40001"}, KindTransient},
		{"insufficient privilege is config, not row", &pgconn.PgError{Code: "42501"}, KindTransient},
		{"network error", errors.New("connection refused"), KindTransient},
		{"context deadline", context.DeadlineExceeded, KindTransient},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyDBError(c.err); got != c.want {
				t.Fatalf("got %s want %s", got, c.want)
			}
		})
	}
}
