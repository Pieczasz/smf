// Package schema provides the Parser interface for reading schema files
// in various formats (TOML, JSON, YAML, etc.) and converting them to
// the canonical core.Database representation.
package schema

import (
	"io"
	"path/filepath"

	"smf/internal/core"
	"smf/internal/parser/toml"
)

type Parser interface {
	Parse(r io.Reader) (*core.Database, error)
	ParseFile(path string) (*core.Database, error)
}

func ParseFile(path string) (*core.Database, error) {
	ext := filepath.Ext(path)

	switch ext {
	case ".toml":
		return toml.NewParser().ParseFile(path)
	default:
		return nil, &UnsupportedFormatError{Path: path}
	}
}

type UnsupportedFormatError struct {
	Path string
}

func (e *UnsupportedFormatError) Error() string {
	return "unsupported file format: " + e.Path
}
