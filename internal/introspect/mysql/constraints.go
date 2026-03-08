package mysql

import (
	"errors"

	"smf/internal/core"
)

// parseConstraint parses a table-level constraint from a CREATE TABLE body item.
//
// Handles: PRIMARY KEY, UNIQUE KEY, FOREIGN KEY, CHECK, and named CONSTRAINT declarations.
//
// Example input: "PRIMARY KEY (`id`)"
// Example input: "CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)".
func parseConstraint(_ core.Dialect, item string) (*core.Constraint, error) {
	// TODO: implement full constraint parsing.
	_ = item
	return nil, errors.New("parseConstraint not yet implemented")
}
