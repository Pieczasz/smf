package diff

import (
	"fmt"
	"sort"
	"strings"

	"smf/internal/core"
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

func (m columnAttrMatch) allMatch() bool {
	return m.TypeRaw && m.Nullable && m.PrimaryKey && m.AutoIncrement &&
		m.Charset && m.Collate && m.Comment && m.DefaultValue && m.OnUpdate &&
		m.IsGenerated && m.GenerationExpression && m.GenerationStorage &&
		m.ColumnFormat && m.Storage && m.AutoRandom
}

// SimilarityScore function calculates a similarity score between two column attributes.
// It is used to detect renames between two columns.
func (m columnAttrMatch) similarityScore() int {
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

// Named is implemented by types that have a name identifier.
// This interface enables type-safe sorting and mapping operations.
type Named interface {
	GetName() string
}

// sortNamed sorts a slice of Named items by name (case-insensitive).
func sortNamed[T Named](items []T) {
	if len(items) <= 1 {
		return
	}
	// Pre-compute lowercase keys once
	keys := make([]string, len(items))
	for i, item := range items {
		keys[i] = strings.ToLower(item.GetName())
	}
	sort.Slice(items, func(i, j int) bool {
		return keys[i] < keys[j]
	})
}

// sortByFunc sorts items using a custom name extractor function.
func sortByFunc[T any](items []T, getName func(T) string) {
	if len(items) <= 1 {
		return
	}
	keys := make([]string, len(items))
	for i, item := range items {
		keys[i] = strings.ToLower(getName(item))
	}
	sort.Slice(items, func(i, j int) bool {
		return keys[i] < keys[j]
	})
}

// mapTablesByName creates a lookup map of tables keyed by lowercase name.
// Returns the map and any case-insensitive name collisions found.
func mapTablesByName(tables []*core.Table) (map[string]*core.Table, []string) {
	m := make(map[string]*core.Table, len(tables))
	original := make(map[string]string, len(tables))
	var collisions []string

	for _, t := range tables {
		key := strings.ToLower(t.Name)
		if prev, ok := original[key]; ok {
			if prev != t.Name {
				collisions = append(collisions, fmt.Sprintf("case-insensitive name collision: %q vs %q", prev, t.Name))
			}
			continue
		}
		original[key] = t.Name
		m[key] = t
	}
	return m, collisions
}

// mapColumnsByName creates a lookup map of columns keyed by lowercase name.
// Returns the map and any case-insensitive name collisions found.
func mapColumnsByName(columns []*core.Column) (map[string]*core.Column, []string) {
	m := make(map[string]*core.Column, len(columns))
	original := make(map[string]string, len(columns))
	var collisions []string

	for _, c := range columns {
		key := strings.ToLower(c.Name)
		if prev, ok := original[key]; ok {
			if prev != c.Name {
				collisions = append(collisions, fmt.Sprintf("case-insensitive name collision: %q vs %q", prev, c.Name))
			}
			continue
		}
		original[key] = c.Name
		m[key] = c
	}
	return m, collisions
}

// mapConstraintsByKey creates a lookup map of constraints keyed by a custom key function.
func mapConstraintsByKey(items []*core.Constraint, keyFn func(*core.Constraint) string) map[string]*core.Constraint {
	m := make(map[string]*core.Constraint, len(items))
	for _, item := range items {
		m[keyFn(item)] = item
	}
	return m
}

// mapIndexesByKey creates a lookup map of indexes keyed by a custom key function.
func mapIndexesByKey(items []*core.Index, keyFn func(*core.Index) string) map[string]*core.Index {
	m := make(map[string]*core.Index, len(items))
	for _, item := range items {
		m[keyFn(item)] = item
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
	if len(keys) <= 1 {
		return keys
	}
	lowerKeys := make([]string, len(keys))
	for i, k := range keys {
		lowerKeys[i] = strings.ToLower(k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return lowerKeys[i] < lowerKeys[j]
	})
	return keys
}
