package auth

import (
	"errors"

	"golang.org/x/crypto/bcrypt"
)

const defaultPasswordCost = 10

var (
	ErrPasswordTooLong = errors.New("password is too long (max 72 bytes)")
	ErrInvalidHash = errors.New("invalid bcrypt hash format")
)

func Hash(password string) (string, error) {
    if len(password) > 72 {
        return "", ErrPasswordTooLong
    }
    
	hash, err := bcrypt.GenerateFromPassword([]byte(password), defaultPasswordCost)
	if err != nil {
		return "", err
	}
    
    return string(hash), nil
}

func Check(password, hash string) error {
	if len(hash) < 60 {
		return ErrInvalidHash
	}

	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

func NeedsRehash(hash string) bool {
    cost, err := bcrypt.Cost([]byte(hash))
    if err != nil {
        return true
    }
    return cost < defaultPasswordCost
}