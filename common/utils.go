package common

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/devopsext/utils"
	"github.com/google/uuid"
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

	// 1. Return early if the map is empty to save time
	if len(m) == 0 {
		return ""
	}

	// 2. Pre-allocate slice capacity to avoid re-allocations during append
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	// 3. Sort the keys
	sort.Strings(keys)

	// 4. Pre-size the builder's buffer if you want to go ultra-optimized.
	// (Optional, but helps if the strings are exceptionally large)
	var builder strings.Builder

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

func Map2Hash64(m map[string]string) uint64 {

	s := MapToString(m)
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func Map2Hash32Slow(m map[string]string) uint32 {

	// 1. Return early if the map is empty to save time
	if len(m) == 0 {
		return 0
	}

	// 2. Pre-allocate slice capacity to avoid re-allocations during append
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	h := fnv.New32a()

	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte("="))
		h.Write([]byte(m[k]))
		h.Write([]byte(";"))
	}

	return h.Sum32()
}

func String2Hash32Slow(s string) uint32 {

	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

const (
	offset32 = 2166136261
	prime32  = 16777619
)

func Map2Hash32(m map[string]string) uint32 {
	if len(m) == 0 {
		return 0
	}

	// 1. Allocate once and collect keys
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	// 2. Sort keys to guarantee deterministic output
	slices.Sort(keys)

	// 3. Unrolled FNV-1a calculation
	hash := uint32(offset32)

	for _, k := range keys {
		v := m[k]

		for i := 0; i < len(k); i++ {
			hash ^= uint32(k[i])
			hash *= prime32
		}

		hash ^= uint32('=')
		hash *= prime32

		for i := 0; i < len(v); i++ {
			hash ^= uint32(v[i])
			hash *= prime32
		}

		hash ^= uint32(';')
		hash *= prime32
	}

	return hash
}

func String2Hash32(s string) uint32 {
	hash := uint32(offset32)
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= prime32
	}
	return hash
}

/*func ApplicationsCompact(apps []*Application) []*Application {

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
}*/

func packTo64(h1, h2 uint32) uint64 {
	return (uint64(h1) << 32) | uint64(h2)
}

// Unpack is optional, but helpful if you ever need the original hashes back
func unpackFrom64(packed uint64) (uint32, uint32) {
	h1 := uint32(packed >> 32)
	h2 := uint32(packed) // Truncates the upper 32 bits automatically
	return h1, h2
}

func StampToTime(stamp Stamp) time.Time {
	return time.UnixMilli(int64(stamp))
}

func TimeToStamp(time time.Time) Stamp {
	return Stamp(time.UnixMilli())
}

func DurationToString(duration time.Duration) string {

	d := duration.Round(time.Second)

	totalSeconds := int64(d.Seconds())

	// 2. Break down into days, hours, minutes, and seconds
	days := totalSeconds / (24 * 3600)
	totalSeconds %= (24 * 3600)

	hours := totalSeconds / 3600
	totalSeconds %= 3600

	minutes := totalSeconds / 60
	seconds := totalSeconds % 60

	var result strings.Builder
	// Only append the unit if the value is strictly greater than 0
	if days > 0 {
		result.WriteString(fmt.Sprintf("%dd", days))
	}
	if hours > 0 {
		result.WriteString(fmt.Sprintf("%dh", hours))
	}
	if minutes > 0 {
		result.WriteString(fmt.Sprintf("%dm", minutes))
	}
	if seconds > 0 {
		result.WriteString(fmt.Sprintf("%ds", seconds))
	}

	// TrimSpace removes any trailing space if seconds was 0
	s := strings.TrimSpace(result.String())
	if s == "" {
		s = "0"
	}
	return s
}

func DefaultTTL(ttl string, def time.Duration) time.Duration {

	r := def
	if !utils.IsEmpty(ttl) {
		d, err := time.ParseDuration(ttl)
		if err == nil {
			r = d
		}
	}
	return r
}

func UniqueID() string {
	id := uuid.New()
	return id.String()
}

type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

type Aggregator[T Number] = func(slice []T) T

func Sum[T Number](slice []T) T {
	var total T
	for _, v := range slice {
		total += v
	}
	return total
}

func Avg[T Number](slice []T) T {

	var avg T
	l := Len(slice)
	if l == 0 {
		return avg
	}

	total := Sum(slice)
	avg = total / T(l)
	return avg
}

func Max[T Number](slice []T) T {

	var max T
	for _, v := range slice {
		if max < v {
			max = v
		}
	}
	return max
}

func Len[T any](s []T) int {
	return len(s)
}

func MetricLabelsPath(metric string, labels Labels) string {

	if metric == "" {
		return ""
	}

	iter := maps.Keys(labels)
	keys := slices.Collect(iter)
	sort.Strings(keys)

	var builder strings.Builder
	builder.WriteString(metric)

	if len(keys) == 0 {
		return builder.String()
	}
	builder.WriteString("/")

	for idx, k := range keys {
		builder.WriteString(labels[k])
		if idx < len(keys)-1 {
			builder.WriteString("/")
		}
	}

	return builder.String()
}
