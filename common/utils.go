package common

import (
	"strings"

	"github.com/devopsext/utils"
)

func RemoveEmptyStrings(items []string) []string {

	r := []string{}

	for _, v := range items {
		if utils.IsEmpty(v) {
			continue
		}
		r = append(r, strings.TrimSpace(v))
	}

	return r
}

func GetStringKeys(arr map[string]interface{}) []string {
	var keys []string
	for k := range arr {
		keys = append(keys, k)
	}
	return keys
}
