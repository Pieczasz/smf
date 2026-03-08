package mysql

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"smf/internal/core"
	"smf/internal/introspect"
)

// bodyItemKind classifies a single item inside the CREATE TABLE body.
type bodyItemKind int

const (
	bodyItemColumn     bodyItemKind = iota // Column definition (starts with identifier token).
	bodyItemConstraint                     // Table-level PK, UNIQUE, FK, CHECK, or named CONSTRAINT.
	bodyItemIndex                          // Inline index: KEY, INDEX, FULLTEXT, SPATIAL.
)

// ddlSections holds the three top-level sections of a CREATE TABLE statement.
type ddlSections struct {
	head string // Everything before the opening '(' (e.g. "CREATE TABLE `t`").
	body string // Content inside the outer parentheses.
	tail string // Table options after the closing ')'.
}

func introspectTables(ic *introspectCtx, db *core.Database) error {
	tableNames, err := queryTableNames(ic)
	if err != nil {
		return err
	}
	if len(tableNames) == 0 {
		return nil
	}

	tables, err := queryTablesData(ic.ctx, ic, tableNames)
	if err != nil {
		return err
	}

	db.Tables = append(db.Tables, tables...)
	return nil
}
func queryTableNames(ic *introspectCtx) ([]string, error) {
	query := `
        SELECT TABLE_NAME
        FROM information_schema.tables
        WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_TYPE = 'BASE TABLE'
    `
	rows, err := ic.db.QueryContext(ic.ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tableNames, nil
}

func queryTablesData(parentCtx context.Context, ic *introspectCtx, names []string) ([]*core.Table, error) {
	// errgroup.WithContext automatically cancels the ctx on the first error
	g, ctx := errgroup.WithContext(parentCtx)

	tables := make([]*core.Table, 0, len(names))
	var mu sync.Mutex

	jobs := make(chan string)

	workers := computeWorkerCount(len(names))
	for range workers {
		g.Go(func() error {
			for tableName := range jobs {
				table, err := introspectTable(ctx, ic, tableName)
				if err != nil {
					return fmt.Errorf("introspect table %s: %w", tableName, err)
				}

				mu.Lock()
				tables = append(tables, table)
				mu.Unlock()
			}
			return nil
		})
	}

	g.Go(func() error {
		defer close(jobs)
		for _, tableName := range names {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobs <- tableName:
			}
		}
		return nil
	})

	// Wait blocks until all g.Go() functions finish.
	// It returns the first error encountered, if any.
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return tables, nil
}

func computeWorkerCount(n int) int {
	if n <= 0 {
		return 0
	}
	workers := min(n, max(2, runtime.NumCPU()))
	return min(workers, 8)
}

func introspectTable(ctx context.Context, ic *introspectCtx, tableName string) (*core.Table, error) {
	ddl, err := queryShowCreateTable(ctx, ic, tableName)
	if err != nil {
		return nil, fmt.Errorf("show create table: %w", err)
	}

	table, err := parseCreateTableDDL(ic.dialect, tableName, ddl)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	return table, nil
}

func queryShowCreateTable(ctx context.Context, ic *introspectCtx, tableName string) (string, error) {
	query := "SHOW CREATE TABLE " + core.QuoteMySQLIdentifier(tableName)
	var ignored string
	var ddl string
	if err := ic.db.QueryRowContext(ctx, query).Scan(&ignored, &ddl); err != nil {
		return "", err
	}
	return ddl, nil
}

func parseCreateTableDDL(dialect core.Dialect, tableName, ddl string) (*core.Table, error) {
	table := &core.Table{
		Name:        tableName,
		Columns:     make([]*core.Column, 0),
		Constraints: make([]*core.Constraint, 0),
		Indexes:     make([]*core.Index, 0),
	}

	switch dialect {
	case core.DialectMySQL:
		table.Options.MySQL = &core.MySQLTableOptions{}
	case core.DialectMariaDB:
		table.Options.MySQL = &core.MySQLTableOptions{}
		table.Options.MariaDB = &core.MariaDBTableOptions{}
	case core.DialectTiDB:
		table.Options.MySQL = &core.MySQLTableOptions{}
		table.Options.TiDB = &core.TiDBTableOptions{}
	}

	sections, err := splitDDLSections(ddl)
	if err != nil {
		return nil, err
	}

	items := introspect.SplitBy(sections.body, ',')

	for _, item := range items {
		switch classifyBodyItem(item) {
		case bodyItemColumn:
			col, err := parseColumn(dialect, item)
			if err != nil {
				return nil, fmt.Errorf("parse column: %w", err)
			}
			table.Columns = append(table.Columns, col)

		case bodyItemConstraint:
			constraint, err := parseConstraint(dialect, item)
			if err != nil {
				return nil, fmt.Errorf("parse constraint: %w", err)
			}
			table.Constraints = append(table.Constraints, constraint)

		case bodyItemIndex:
			idx, err := parseIndex(dialect, item)
			if err != nil {
				return nil, fmt.Errorf("parse index: %w", err)
			}
			table.Indexes = append(table.Indexes, idx)
		}
	}

	// TODO: parse sections.tail for table options (ENGINE, CHARSET, etc.)
	_ = sections.tail

	return table, nil
}

// splitDDLSections splits a CREATE TABLE statement into head, body, and tail.
func splitDDLSections(ddl string) (ddlSections, error) {
	openIdx := -1
	for i, ch := range ddl {
		if ch == '(' {
			openIdx = i
			break
		}
	}
	if openIdx == -1 {
		return ddlSections{}, errors.New("missing opening parenthesis in CREATE TABLE DDL")
	}

	closeIdx, err := introspect.FindMatchingParen(ddl, openIdx)
	if err != nil || closeIdx == -1 {
		return ddlSections{}, errors.New("unbalanced parentheses in CREATE TABLE DDL")
	}

	return ddlSections{
		head: strings.TrimSpace(ddl[:openIdx]),
		body: ddl[openIdx+1 : closeIdx],
		tail: strings.TrimSpace(ddl[closeIdx+1:]),
	}, nil
}

// classifyBodyItem determines whether a body item is a column definition.
func classifyBodyItem(item string) bodyItemKind {
	upper := strings.ToUpper(strings.TrimSpace(item))

	if strings.HasPrefix(upper, "CONSTRAINT") {
		return bodyItemConstraint
	}

	for _, prefix := range []string{
		"PRIMARY KEY",
		"UNIQUE KEY", "UNIQUE INDEX", "UNIQUE ",
		"FOREIGN KEY",
		"CHECK",
	} {
		if strings.HasPrefix(upper, prefix) {
			return bodyItemConstraint
		}
	}

	for _, prefix := range []string{
		"KEY", "INDEX",
		"FULLTEXT KEY", "FULLTEXT INDEX", "FULLTEXT",
		"SPATIAL KEY", "SPATIAL INDEX", "SPATIAL",
	} {
		if strings.HasPrefix(upper, prefix) {
			return bodyItemIndex
		}
	}

	return bodyItemColumn
}
