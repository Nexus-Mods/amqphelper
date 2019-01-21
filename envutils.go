package amqphelper

import (
	"os"
	"strconv"
)

func GetenvStr(key string) string {
	v := os.Getenv(key)
	return v
}

func GetenvInt(key string) int {
	s := GetenvStr(key)
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return v
}

func GetenvBool(key string) bool {
	s := GetenvStr(key)
	v, err := strconv.ParseBool(s)
	if err != nil {
		return false
	}
	return v
}