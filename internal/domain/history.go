package domain

const (
	DefaultHistoryLimit = 50
	MaxHistoryLimit     = 100
)

func NormalizeHistoryLimit(limit int) (int, error) {
	if limit == 0 {
		return DefaultHistoryLimit, nil
	}
	if limit < 0 || limit > MaxHistoryLimit {
		return 0, ErrInvalidInput
	}
	return limit, nil
}
