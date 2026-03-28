package mysql

import (
	"errors"

	"smf/internal/introspect"
	"smf/internal/schema"
)

// parseConstraint parses a table-level constraint from a CREATE TABLE body item.
//
// Handles: PRIMARY KEY, UNIQUE KEY, FOREIGN KEY, CHECK, and named CONSTRAINT declarations.
//
// Example input: "PRIMARY KEY (`id`)"
// Example input: "CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)".
func parseConstraint(_ schema.Dialect, item string) (*schema.Constraint, error) {
	tokens := introspect.Tokenize(item)

	if len(tokens) == 0 {
		return nil, nil
	}

	_ = item
	return nil, errors.New("parseConstraint not yet implemented")
}
