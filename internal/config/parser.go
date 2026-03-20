// Package config provides the Parser interface for reading schema files
// in various formats (TOML, JSON, YAML, etc.) and converting them to
// the canonical schema.Database representation.
package config

import (
	"fmt"
	"io"
	"path/filepath"

	"smf/internal/config/toml"
	"smf/internal/schema"
)

type Parser interface {
	Parse(r io.Reader) (*schema.Database, error)
}

func ParseFile(path string) (*schema.Database, error) {
	ext := filepath.Ext(path)

	switch ext {
	case ".toml":
		return toml.NewParser().ParseFile(path)
	default:
		return nil, fmt.Errorf("unsupported file format: %v", ext)
	}
}
