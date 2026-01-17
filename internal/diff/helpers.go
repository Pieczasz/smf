package diff

import (
	"smf/internal/core"
	"sort"
	"strconv"
	"strings"
)

type columnAttrMatch struct {
	TypeRaw              bool
	Type                 bool
	Nullable             bool
	PrimaryKey           bool
	AutoIncrement        bool
	Charset              bool
	Collate              bool
	Comment              bool
	DefaultValue         bool
	OnUpdate             bool
	IsGenerated          bool
	GenerationExpression bool
	GenerationStorage    bool
	ColumnFormat         bool
	Storage              bool
	AutoRandom           bool
}

func compareColumnAttrs(a, b *core.Column) columnAttrMatch {
	return columnAttrMatch{
		TypeRaw:              strings.EqualFold(a.TypeRaw, b.TypeRaw),
		Type:                 a.Type == b.Type,
		Nullable:             a.Nullable == b.Nullable,
		PrimaryKey:           a.PrimaryKey == b.PrimaryKey,
		AutoIncrement:        a.AutoIncrement == b.AutoIncrement,
		Charset:              strings.EqualFold(strings.TrimSpace(a.Charset), strings.TrimSpace(b.Charset)),
		Collate:              strings.EqualFold(strings.TrimSpace(a.Collate), strings.TrimSpace(b.Collate)),
		Comment:              strings.EqualFold(a.Comment, b.Comment),
		DefaultValue:         ptrEq(a.DefaultValue, b.DefaultValue),
		OnUpdate:             ptrEq(a.OnUpdate, b.OnUpdate),
		IsGenerated:          a.IsGenerated == b.IsGenerated,
		GenerationExpression: strings.TrimSpace(a.GenerationExpression) == strings.TrimSpace(b.GenerationExpression),
		GenerationStorage:    strings.EqualFold(string(a.GenerationStorage), string(b.GenerationStorage)),
		ColumnFormat:         strings.EqualFold(a.ColumnFormat, b.ColumnFormat),
		Storage:              strings.EqualFold(a.Storage, b.Storage),
		AutoRandom:           a.AutoRandom == b.AutoRandom,
	}
}

func (m columnAttrMatch) AllMatch() bool {
	return m.TypeRaw && m.Nullable && m.PrimaryKey && m.AutoIncrement &&
		m.Charset && m.Collate && m.Comment && m.DefaultValue && m.OnUpdate &&
		m.IsGenerated && m.GenerationExpression && m.GenerationStorage &&
		m.ColumnFormat && m.Storage && m.AutoRandom
}

func (m columnAttrMatch) SimilarityScore() int {
	score := 0
	if m.TypeRaw {
		score += 4
	}
	if m.Type {
		score += 2
	}
	if m.Nullable {
		score += 1
	}
	if m.AutoIncrement {
		score += 1
	}
	if m.PrimaryKey {
		score += 1
	}
	if m.DefaultValue {
		score += 1
	}
	if m.Charset {
		score += 1
	}
	if m.Collate {
		score += 1
	}
	if m.IsGenerated {
		score += 1
	}
	if m.GenerationExpression {
		score += 1
	}
	if m.GenerationStorage {
		score += 1
	}
	if m.Comment {
		score += 1
	}
	return score
}

type fieldChangeCollector struct {
	Changes []*FieldChange
}

func (c *fieldChangeCollector) Add(field, oldV, newV string) {
	if oldV == newV {
		return
	}
	c.Changes = append(c.Changes, &FieldChange{Field: field, Old: oldV, New: newV})
}

func sortByNameCI[T any](items []T, name func(T) string) {
	sort.Slice(items, func(i, j int) bool {
		return strings.ToLower(name(items[i])) < strings.ToLower(name(items[j]))
	})
}

func mapByLowerName[T any](items []T, name func(T) string) map[string]T {
	m := make(map[string]T, len(items))
	for _, item := range items {
		m[strings.ToLower(name(item))] = item
	}
	return m
}

func mapByKey[T any](items []T, key func(T) string) map[string]T {
	m := make(map[string]T, len(items))
	for _, item := range items {
		m[key(item)] = item
	}
	return m
}

func equalStringSliceCI(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

func u64(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func ptrEq(a, b *string) bool {
	return ptrStr(a) == ptrStr(b)
}

func formatNameList(items []string) string {
	return "(" + strings.Join(items, ", ") + ")"
}

func unionKeys(a, b map[string]string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return strings.ToLower(keys[i]) < strings.ToLower(keys[j])
	})
	return keys
}
