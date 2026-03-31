package common

import (
	"maps"
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

func GetStringKeys(arr map[string]string) []string {
	var keys []string
	for k := range arr {
		keys = append(keys, k)
	}
	return keys
}

func MergeStringMaps(mm ...map[string]string) map[string]string {

	r := make(map[string]string)
	for _, m := range mm {
		maps.Copy(r, m)
	}
	return r
}
