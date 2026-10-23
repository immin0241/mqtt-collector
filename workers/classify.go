package workers

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

type ErrorKind string

const (
	// KindTransient는 같은 입력으로 재시도하면 성공할 수 있는 오류다. (연결, 타임아웃, 장애 조치 등)
	KindTransient ErrorKind = "transient"
	// KindPermanent는 입력 자체가 문제라 재시도해도 같은 결과가 나오는 오류다. (데이터 오류, 제약 위반)
	KindPermanent ErrorKind = "permanent"
)

// ClassifyDBError는 SQLSTATE 클래스로 오류를 분류한다.
//
//	22 data exception, 23 integrity constraint violation -> permanent
//	그 외 PgError(08, 40, 53, 57, 58, 42 ...) 및 PgError가 아닌 오류(네트워크, context) -> transient
//
// 알 수 없는 오류를 transient로 두는 이유: 잘못 permanent로 분류하면 정상 행이 격리되지만,
// 잘못 transient로 분류하면 수신이 멈출 뿐 데이터는 잃지 않는다. (불변식 2)
func ClassifyDBError(err error) ErrorKind {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 {
		switch pgErr.Code[:2] {
		case "22", "23":
			return KindPermanent
		}
	}
	return KindTransient
}

// sqlStateOf는 err에서 SQLSTATE를 꺼낸다. PgError가 아니면 "UNKNOWN".
func sqlStateOf(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return "UNKNOWN"
}

func derefOr[T any](p *T, fallback T) T {
	if p == nil {
		return fallback
	}
	return *p
}
