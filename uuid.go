package gocelery

import (
	"github.com/satori/go.uuid"
)

func GetUuidString() string {
	uuid, _ := uuid.NewV4()

	return uuid.String();
}