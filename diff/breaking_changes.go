package diff

import (
	"fmt"
	"regexp"
	"smf/core"
	"strconv"
	"strings"
)

type BreakingChange struct {
	Severity    ChangeSeverity
	Description string
	Table       string
	Object      string
	ObjectType  string
}

type ChangeSeverity int

const (
	SeverityInfo ChangeSeverity = iota
	SeverityWarning
	SeverityBreaking
	SeverityCritical
)

func (s ChangeSeverity) String() string {
	switch s {
	case SeverityInfo:
		return "INFO"
	case SeverityWarning:
		return "WARNING"
	case SeverityBreaking:
		return "BREAKING"
	case SeverityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

type BreakingChangeAnalyzer struct {
	Changes []BreakingChange
}

func NewBreakingChangeAnalyzer() *BreakingChangeAnalyzer {
	return &BreakingChangeAnalyzer{}
}

func (a *BreakingChangeAnalyzer) Analyze(diff *SchemaDiff) []BreakingChange {
	if diff == nil {
		return nil
	}

	a.analyzeRemovedTables(diff.RemovedTables)
	a.analyzeModifiedTables(diff.ModifiedTables)

	return a.Changes
}

func (a *BreakingChangeAnalyzer) analyzeRemovedTables(tables []*core.Table) {
	for _, t := range tables {
		a.add(BreakingChange{
			Severity:    SeverityCritical,
			Description: "Table will be dropped - all data will be lost",
			Table:       t.Name,
			Object:      t.Name,
			ObjectType:  "TABLE",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeModifiedTables(tables []*TableDiff) {
	for _, td := range tables {
		a.analyzeRenamedColumns(td.Name, td.RenamedColumns)
		a.analyzeRemovedColumns(td.Name, td.RemovedColumns)
		a.analyzeModifiedColumns(td.Name, td.ModifiedColumns)
		a.analyzeAddedColumns(td.Name, td.AddedColumns)
		a.analyzeRemovedConstraints(td.Name, td.RemovedConstraints)
		a.analyzeModifiedConstraints(td.Name, td.ModifiedConstraints)
		a.analyzeAddedConstraints(td.Name, td.AddedConstraints)
		a.analyzeRemovedIndexes(td.Name, td.RemovedIndexes)
		a.analyzeModifiedIndexes(td.Name, td.ModifiedIndexes)
		a.analyzeAddedIndexes(td.Name, td.AddedIndexes)
		a.analyzeModifiedOptions(td.Name, td.ModifiedOptions)
	}
}

func (a *BreakingChangeAnalyzer) analyzeRenamedColumns(table string, renames []*ColumnRename) {
	for _, r := range renames {
		a.add(BreakingChange{
			Severity:    SeverityBreaking,
			Description: fmt.Sprintf("Column rename detected: %s -> %s (handled as CHANGE COLUMN to preserve data; review type/attrs carefully)", r.Old.Name, r.New.Name),
			Table:       table,
			Object:      fmt.Sprintf("%s->%s", r.Old.Name, r.New.Name),
			ObjectType:  "COLUMN_RENAME",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeRemovedColumns(table string, columns []*core.Column) {
	for _, c := range columns {
		a.add(BreakingChange{
			Severity:    SeverityCritical,
			Description: "Column will be dropped - data will be lost",
			Table:       table,
			Object:      c.Name,
			ObjectType:  "COLUMN",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeModifiedColumns(table string, changes []*ColumnChange) {
	for _, ch := range changes {
		a.analyzeTypeChange(table, ch)
		a.analyzeColumnLengthChange(table, ch)
		a.analyzeNullabilityChange(table, ch)
		a.analyzeAutoIncrementChange(table, ch)
		a.analyzePrimaryKeyChange(table, ch)
		a.analyzeGeneratedColumnChange(table, ch)
		a.analyzeCharsetCollateChange(table, ch)
		a.analyzeDefaultValueChange(table, ch)
		a.analyzeCommentChange(table, ch)
	}
}

func (a *BreakingChangeAnalyzer) analyzeTypeChange(table string, ch *ColumnChange) {
	oldType, newType := ch.Old.TypeRaw, ch.New.TypeRaw

	if oldBase, oldLen, okOld := parseTypeLength(oldType); okOld {
		if newBase, newLen, okNew := parseTypeLength(newType); okNew {
			if strings.EqualFold(oldBase, newBase) && oldLen != newLen {
				switch strings.ToLower(strings.TrimSpace(oldBase)) {
				case "varchar", "char":
					return
				}
			}
		}
	}
	if strings.EqualFold(oldType, newType) {
		return
	}

	severity := a.determineTypeMigrationSeverity(strings.ToLower(oldType), strings.ToLower(newType))

	a.add(BreakingChange{
		Severity:    severity,
		Description: fmt.Sprintf("Column type changes from %s to %s", oldType, newType),
		Table:       table,
		Object:      ch.Name,
		ObjectType:  "COLUMN",
	})
}

var reTypeLen = regexp.MustCompile(`(?i)^\s*([a-z0-9_]+)\s*\(\s*(\d+)\s*\)`) // e.g. varchar(255)

func (a *BreakingChangeAnalyzer) analyzeColumnLengthChange(table string, ch *ColumnChange) {
	oldBase, oldLen, okOld := parseTypeLength(ch.Old.TypeRaw)
	newBase, newLen, okNew := parseTypeLength(ch.New.TypeRaw)
	if !okOld || !okNew {
		return
	}
	if !strings.EqualFold(oldBase, newBase) {
		return
	}
	if oldLen == newLen {
		return
	}

	if oldLen > newLen {
		a.add(BreakingChange{
			Severity:    SeverityBreaking,
			Description: fmt.Sprintf("Column length shrinks from %s(%d) to %s(%d) - existing values may be truncated", oldBase, oldLen, newBase, newLen),
			Table:       table,
			Object:      ch.Name,
			ObjectType:  "COLUMN",
		})
		return
	}

	a.add(BreakingChange{
		Severity:    SeverityInfo,
		Description: fmt.Sprintf("Column length increases from %s(%d) to %s(%d)", oldBase, oldLen, newBase, newLen),
		Table:       table,
		Object:      ch.Name,
		ObjectType:  "COLUMN",
	})
}

func parseTypeLength(typeRaw string) (base string, length int, ok bool) {
	typeRaw = strings.TrimSpace(typeRaw)
	m := reTypeLen.FindStringSubmatch(typeRaw)
	if len(m) != 3 {
		return "", 0, false
	}
	base = strings.ToLower(strings.TrimSpace(m[1]))
	// Only length-sensitive types for now.
	switch base {
	case "varchar", "char", "varbinary", "binary":
		// ok
	default:
		return "", 0, false
	}
	n, err := strconv.Atoi(m[2])
	if err != nil {
		return "", 0, false
	}
	return base, n, true
}

func (a *BreakingChangeAnalyzer) determineTypeMigrationSeverity(oldType, newType string) ChangeSeverity {
	if a.isWideningConversion(oldType, newType) {
		return SeverityInfo
	}
	if a.isNarrowingConversion(oldType, newType) {
		return SeverityCritical
	}
	if a.isIncompatibleConversion(oldType, newType) {
		return SeverityCritical
	}
	return SeverityBreaking
}

func (a *BreakingChangeAnalyzer) isWideningConversion(old, new string) bool {
	widening := map[string][]string{
		"tinyint":    {"smallint", "mediumint", "int", "bigint"},
		"smallint":   {"mediumint", "int", "bigint"},
		"mediumint":  {"int", "bigint"},
		"int":        {"bigint"},
		"float":      {"double", "decimal"},
		"varchar":    {"text", "mediumtext", "longtext"},
		"char":       {"varchar", "text"},
		"tinytext":   {"text", "mediumtext", "longtext"},
		"text":       {"mediumtext", "longtext"},
		"mediumtext": {"longtext"},
	}

	for oldType, widerTypes := range widening {
		if strings.Contains(old, oldType) {
			for _, wider := range widerTypes {
				if strings.Contains(new, wider) {
					return true
				}
			}
		}
	}
	return false
}

func (a *BreakingChangeAnalyzer) isNarrowingConversion(old, new string) bool {
	return a.isWideningConversion(new, old)
}

func (a *BreakingChangeAnalyzer) isIncompatibleConversion(old, new string) bool {
	incompatible := [][2]string{
		{"int", "varchar"},
		{"varchar", "int"},
		{"text", "int"},
		{"datetime", "int"},
		{"json", "int"},
		{"blob", "text"},
	}

	for _, pair := range incompatible {
		if strings.Contains(old, pair[0]) && strings.Contains(new, pair[1]) {
			return true
		}
	}
	return false
}

func (a *BreakingChangeAnalyzer) analyzeNullabilityChange(table string, ch *ColumnChange) {
	if ch.Old.Nullable && !ch.New.Nullable {
		a.add(BreakingChange{
			Severity:    SeverityBreaking,
			Description: "Column becomes NOT NULL - existing NULL values will cause migration failure",
			Table:       table,
			Object:      ch.Name,
			ObjectType:  "COLUMN",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeAutoIncrementChange(table string, ch *ColumnChange) {
	if ch.Old.AutoIncrement && !ch.New.AutoIncrement {
		a.add(BreakingChange{
			Severity:    SeverityWarning,
			Description: "AUTO_INCREMENT is being removed - new inserts will require explicit values",
			Table:       table,
			Object:      ch.Name,
			ObjectType:  "COLUMN",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzePrimaryKeyChange(table string, ch *ColumnChange) {
	if ch.Old.PrimaryKey != ch.New.PrimaryKey {
		a.add(BreakingChange{
			Severity:    SeverityBreaking,
			Description: "Primary key status changed - may fail with duplicates or foreign key references",
			Table:       table,
			Object:      ch.Name,
			ObjectType:  "COLUMN",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeGeneratedColumnChange(table string, ch *ColumnChange) {
	oldc, newc := ch.Old, ch.New

	if oldc.IsGenerated != newc.IsGenerated {
		a.add(BreakingChange{
			Severity:    SeverityBreaking,
			Description: "Generated column status changed - may require data migration",
			Table:       table,
			Object:      ch.Name,
			ObjectType:  "COLUMN",
		})
		return
	}

	if !oldc.IsGenerated {
		return
	}

	if strings.TrimSpace(oldc.GenerationExpression) != strings.TrimSpace(newc.GenerationExpression) {
		a.add(BreakingChange{
			Severity:    SeverityBreaking,
			Description: "Generated column expression changed - computed values will differ",
			Table:       table,
			Object:      ch.Name,
			ObjectType:  "COLUMN",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeCharsetCollateChange(table string, ch *ColumnChange) {
	if !strings.EqualFold(ch.Old.Charset, ch.New.Charset) && ch.Old.Charset != "" && ch.New.Charset != "" {
		a.add(BreakingChange{
			Severity:    SeverityWarning,
			Description: fmt.Sprintf("Character set changes from %s to %s - may affect data encoding", ch.Old.Charset, ch.New.Charset),
			Table:       table,
			Object:      ch.Name,
			ObjectType:  "COLUMN",
		})
	}

	if !strings.EqualFold(ch.Old.Collate, ch.New.Collate) && ch.Old.Collate != "" && ch.New.Collate != "" {
		a.add(BreakingChange{
			Severity:    SeverityWarning,
			Description: fmt.Sprintf("Collation changes from %s to %s - may affect sorting and comparisons", ch.Old.Collate, ch.New.Collate),
			Table:       table,
			Object:      ch.Name,
			ObjectType:  "COLUMN",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeDefaultValueChange(table string, ch *ColumnChange) {
	oldV := ptrStr(ch.Old.DefaultValue)
	newV := ptrStr(ch.New.DefaultValue)
	if oldV == newV {
		return
	}
	a.add(BreakingChange{
		Severity:    SeverityWarning,
		Description: fmt.Sprintf("Default value changes from %q to %q", oldV, newV),
		Table:       table,
		Object:      ch.Name,
		ObjectType:  "COLUMN",
	})
}

func (a *BreakingChangeAnalyzer) analyzeCommentChange(table string, ch *ColumnChange) {
	if strings.TrimSpace(ch.Old.Comment) == strings.TrimSpace(ch.New.Comment) {
		return
	}
	a.add(BreakingChange{
		Severity:    SeverityInfo,
		Description: "Column comment changed",
		Table:       table,
		Object:      ch.Name,
		ObjectType:  "COLUMN",
	})
}

func (a *BreakingChangeAnalyzer) analyzeAddedColumns(table string, columns []*core.Column) {
	for _, c := range columns {
		if !c.Nullable && c.DefaultValue == nil && !c.IsGenerated {
			a.add(BreakingChange{
				Severity:    SeverityBreaking,
				Description: "Adding NOT NULL column without default - will fail if table has existing rows",
				Table:       table,
				Object:      c.Name,
				ObjectType:  "COLUMN",
			})
		}
	}
}

func (a *BreakingChangeAnalyzer) analyzeRemovedConstraints(table string, constraints []*core.Constraint) {
	for _, c := range constraints {
		switch c.Type {
		case core.ConstraintPrimaryKey:
			a.add(BreakingChange{
				Severity:    SeverityCritical,
				Description: "Primary key will be dropped - this affects table identity",
				Table:       table,
				Object:      c.Name,
				ObjectType:  "CONSTRAINT",
			})
		case core.ConstraintForeignKey:
			a.add(BreakingChange{
				Severity:    SeverityWarning,
				Description: "Foreign key will be dropped - referential integrity no longer enforced",
				Table:       table,
				Object:      c.Name,
				ObjectType:  "CONSTRAINT",
			})
		case core.ConstraintUnique:
			a.add(BreakingChange{
				Severity:    SeverityWarning,
				Description: "Unique constraint will be dropped - duplicates will be allowed",
				Table:       table,
				Object:      c.Name,
				ObjectType:  "CONSTRAINT",
			})
		case core.ConstraintCheck:
			a.add(BreakingChange{
				Severity:    SeverityInfo,
				Description: "Check constraint will be dropped - validation no longer enforced",
				Table:       table,
				Object:      c.Name,
				ObjectType:  "CONSTRAINT",
			})
		}
	}
}

func (a *BreakingChangeAnalyzer) analyzeModifiedConstraints(table string, changes []*ConstraintChange) {
	for _, ch := range changes {
		if ch.RebuildOnly {
			continue
		}
		name := ch.Name
		if name == "" && ch.New != nil {
			name = string(ch.New.Type)
		}
		a.add(BreakingChange{
			Severity:    SeverityWarning,
			Description: "Constraint modified - may fail if existing data violates new constraint",
			Table:       table,
			Object:      name,
			ObjectType:  "CONSTRAINT",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeAddedConstraints(table string, constraints []*core.Constraint) {
	for _, c := range constraints {
		switch c.Type {
		case core.ConstraintPrimaryKey:
			a.add(BreakingChange{
				Severity:    SeverityBreaking,
				Description: "Primary key added - will fail if duplicates or NULLs exist in key columns",
				Table:       table,
				Object:      c.Name,
				ObjectType:  "CONSTRAINT",
			})
		case core.ConstraintForeignKey:
			a.add(BreakingChange{
				Severity:    SeverityBreaking,
				Description: "Foreign key added - will fail if orphan rows exist",
				Table:       table,
				Object:      c.Name,
				ObjectType:  "CONSTRAINT",
			})
		case core.ConstraintUnique:
			a.add(BreakingChange{
				Severity:    SeverityBreaking,
				Description: "Unique constraint added - will fail if duplicates exist",
				Table:       table,
				Object:      c.Name,
				ObjectType:  "CONSTRAINT",
			})
		case core.ConstraintCheck:
			a.add(BreakingChange{
				Severity:    SeverityBreaking,
				Description: "Check constraint added - will fail if existing rows violate the check",
				Table:       table,
				Object:      c.Name,
				ObjectType:  "CONSTRAINT",
			})
		}
	}
}

func (a *BreakingChangeAnalyzer) analyzeRemovedIndexes(table string, indexes []*core.Index) {
	for _, idx := range indexes {
		a.add(BreakingChange{
			Severity:    SeverityInfo,
			Description: "Index will be dropped - queries may become slower",
			Table:       table,
			Object:      idx.Name,
			ObjectType:  "INDEX",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeAddedIndexes(table string, indexes []*core.Index) {
	for _, idx := range indexes {
		if idx.Unique {
			a.add(BreakingChange{
				Severity:    SeverityBreaking,
				Description: "Unique index added - will fail if duplicates exist",
				Table:       table,
				Object:      idx.Name,
				ObjectType:  "INDEX",
			})
			continue
		}
		a.add(BreakingChange{
			Severity:    SeverityInfo,
			Description: "Index added - may improve query performance but can slow writes",
			Table:       table,
			Object:      idx.Name,
			ObjectType:  "INDEX",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeModifiedIndexes(table string, changes []*IndexChange) {
	for _, ch := range changes {
		severity := SeverityWarning
		// If index becomes unique, existing duplicates will break.
		if !ch.Old.Unique && ch.New.Unique {
			severity = SeverityBreaking
		}
		a.add(BreakingChange{
			Severity:    severity,
			Description: "Index modified - may rebuild index and affect query plans",
			Table:       table,
			Object:      ch.Name,
			ObjectType:  "INDEX",
		})
	}
}

func (a *BreakingChangeAnalyzer) analyzeModifiedOptions(table string, options []*TableOptionChange) {
	for _, opt := range options {
		switch strings.ToUpper(opt.Name) {
		case "ENGINE":
			a.add(BreakingChange{
				Severity:    SeverityBreaking,
				Description: fmt.Sprintf("Storage engine changes from %s to %s - table will be rebuilt", opt.Old, opt.New),
				Table:       table,
				Object:      "ENGINE",
				ObjectType:  "TABLE_OPTION",
			})
		case "CHARSET":
			a.add(BreakingChange{
				Severity:    SeverityWarning,
				Description: fmt.Sprintf("Character set changes from %s to %s - may require data conversion", opt.Old, opt.New),
				Table:       table,
				Object:      "CHARSET",
				ObjectType:  "TABLE_OPTION",
			})
		case "COLLATE":
			a.add(BreakingChange{
				Severity:    SeverityWarning,
				Description: fmt.Sprintf("Collation changes from %s to %s - affects sorting and comparisons", opt.Old, opt.New),
				Table:       table,
				Object:      "COLLATE",
				ObjectType:  "TABLE_OPTION",
			})
		}
	}
}

func (a *BreakingChangeAnalyzer) add(bc BreakingChange) {
	a.Changes = append(a.Changes, bc)
}
