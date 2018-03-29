package gocelery

import (
	"github.com/satori/go.uuid"
)

func GetUuidString() string {
	uuid, error := uuid.NewV4()

	return uuid.String();
}