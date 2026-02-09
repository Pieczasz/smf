// Package dialect provides a unified interface for all database dialects. It is used to
// make sure all SQL dialects are handled in the same way, and we provide complete
// support for all features.
package dialect

import (
	"fmt"
	"maps"
	"sync"

	"smf/internal/core"
	"smf/internal/diff"
	"smf/internal/migration"
)

type Type string

const (
	MySQL      Type = "mysql"
	PostgreSQL Type = "postgresql"
	SQLite     Type = "sqlite"
	MSSQL      Type = "mssql"
	Oracle     Type = "oracle"
)

// Generator interface creates a main abstraction for SQL dialects.
// It contains all methods to support as much as possible for each dialect.
// NOTE: this interface can be changed later if we need more or fewer methods.
type Generator interface {
	GenerateMigration(diff *diff.SchemaDiff, opts MigrationOptions) *migration.Migration
	GenerateCreateTable(table *core.Table) (statement string, fkStatements []string)
	GenerateDropTable(table *core.Table) string
	GenerateAlterTable(diff *diff.TableDiff) []string
	QuoteIdentifier(name string) string
	QuoteString(value string) string
}

// Parser interface is used to parse SQL statements into a database schema.
type Parser interface {
	Parse(sql string) (*core.Database, error)
}

// Dialect interface creates a way to interact with a specific SQL dialect.
type Dialect interface {
	Name() Type
	Generator() Generator
	Parser() Parser
}

var (
	registryMu sync.RWMutex
	registry   = map[Type]func() Dialect{}
)

// RegisterDialect creates a new registry entry for the specified dialect.
func RegisterDialect(d Type, ctor func() Dialect) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[d] = ctor
}

// GetDialect returns the dialect for the specified type from the registry.
func GetDialect(d Type) (Dialect, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	ctor, ok := registry[d]
	if !ok {
		return nil, fmt.Errorf("dialect %q is not registered", d)
	}
	return ctor(), nil
}

// resetRegistry replaces the registry with the given map. Intended for testing only.
func resetRegistry(r map[Type]func() Dialect) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = r
}

// snapshotRegistry returns a shallow copy of the current registry. Intended for testing only.
func snapshotRegistry() map[Type]func() Dialect {
	registryMu.RLock()
	defer registryMu.RUnlock()
	snap := make(map[Type]func() Dialect, len(registry))
	maps.Copy(snap, registry)
	return snap
}

// BreakingChangeDetector provides a way to detect breaking changes between two database schemas.
// This implementation is different for each dialect, as they differ in behavior.
type BreakingChangeDetector interface {
	DetectBreakingChanges(schemaDiff *diff.SchemaDiff) []diff.BreakingChange
}

// MigrationOptions have all possible options that a user can specify during migration.
type MigrationOptions struct {
	Dialect              Type
	IncludeDrops         bool
	IncludeUnsafe        bool
	TransactionMode      TransactionMode
	PreserveForeignKeys  bool
	DeferForeignKeyCheck bool
}

// TransactionMode represents the mode of transaction for migration.
type TransactionMode int

const (
	TransactionNone TransactionMode = iota
	TransactionSingle
	TransactionPerStatement
)

// DefaultMigrationOptions creates a new MigrationOptions instance with default values.
func DefaultMigrationOptions(dialect Type) MigrationOptions {
	return MigrationOptions{
		Dialect:              dialect,
		IncludeDrops:         true,
		IncludeUnsafe:        false,
		TransactionMode:      TransactionSingle,
		PreserveForeignKeys:  true,
		DeferForeignKeyCheck: true,
	}
}
