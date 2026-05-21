package common

import (
	"cmp"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"maps"
	"slices"
	"sort"
	"strings"
	"unsafe"

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

func MergeMaps[M1 map[K]V, K comparable, V any](mm ...M1) M1 {

	r := make(M1)
	for _, m := range mm {
		if len(m) == 0 {
			continue
		}
		maps.Copy(r, m)
	}
	return r
}

func MapToString(m map[string]string) string {

	var builder strings.Builder

	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		builder.WriteString(k)
		builder.WriteString("=")
		builder.WriteString(m[k])
		builder.WriteString(";")
	}

	return builder.String()
}

func MapSha256(m map[string]string) string {

	s := MapToString(m)
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

func MapMD5(m map[string]string) string {

	s := MapToString(m)
	hash := md5.Sum([]byte(s))
	return hex.EncodeToString(hash[:])
}

func MapFNV(m map[string]string) uint64 {

	s := MapToString(m)
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func ApplicationsCompact(apps []*Application) []*Application {

	slices.SortFunc(apps, func(a, b *Application) int {

		if a == nil && b == nil {
			return 0
		}
		if a == nil {
			return 1
		}
		if b == nil {
			return -1
		}
		aInt := uintptr(unsafe.Pointer(a))
		bInt := uintptr(unsafe.Pointer(b))

		return cmp.Compare(aInt, bInt)
	})
	return slices.Compact(apps)
}
