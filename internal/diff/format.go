package diff

import (
	"fmt"
	"os"
	"strings"
)

// String returns a string representation of all schema differences between two sql dumps.
func (d *SchemaDiff) String() string {
	if d.IsEmpty() {
		return "No differences detected."
	}

	var sb strings.Builder
	sb.WriteString("Schema differences:\n")

	if len(d.Warnings) > 0 {
		sb.WriteString("\nWarnings:\n")
		for _, w := range d.Warnings {
			w = strings.TrimSpace(w)
			if w == "" {
				continue
			}
			sb.WriteString(fmt.Sprintf("  - %s\n", w))
		}
	}

	if len(d.AddedTables) > 0 {
		sb.WriteString("\nAdded tables:\n")
		for _, at := range d.AddedTables {
			sb.WriteString(fmt.Sprintf("  - %s\n", at.Name))
		}
	}

	if len(d.RemovedTables) > 0 {
		sb.WriteString("\nRemoved tables:\n")
		for _, rt := range d.RemovedTables {
			sb.WriteString(fmt.Sprintf("  - %s\n", rt.Name))
		}
	}

	if len(d.ModifiedTables) > 0 {
		sb.WriteString("\nModified tables:\n")
		for _, mt := range d.ModifiedTables {
			d.writeTableDiff(&sb, mt)
		}
	}

	return sb.String()
}

func (d *SchemaDiff) writeTableDiff(sb *strings.Builder, mt *TableDiff) {
	sb.WriteString(fmt.Sprintf("\n  - %s\n", mt.Name))

	if len(mt.Warnings) > 0 {
		sb.WriteString("    Warnings:\n")
		for _, w := range mt.Warnings {
			w = strings.TrimSpace(w)
			if w == "" {
				continue
			}
			sb.WriteString(fmt.Sprintf("      - %s\n", w))
		}
	}

	if len(mt.ModifiedOptions) > 0 {
		sb.WriteString("    Options changed:\n")
		for _, mo := range mt.ModifiedOptions {
			sb.WriteString(fmt.Sprintf("      - %s: %q -> %q\n", mo.Name, mo.Old, mo.New))
		}
	}

	if len(mt.AddedColumns) > 0 {
		sb.WriteString("    Added columns:\n")
		for _, ac := range mt.AddedColumns {
			sb.WriteString(fmt.Sprintf("      - %s: %s\n", ac.Name, ac.TypeRaw))
		}
	}

	if len(mt.RemovedColumns) > 0 {
		sb.WriteString("    Removed columns:\n")
		for _, rc := range mt.RemovedColumns {
			sb.WriteString(fmt.Sprintf("      - %s: %s\n", rc.Name, rc.TypeRaw))
		}
	}

	if len(mt.ModifiedColumns) > 0 {
		sb.WriteString("    Modified columns:\n")
		for _, mc := range mt.ModifiedColumns {
			sb.WriteString(fmt.Sprintf("      - %s:\n", mc.Name))
			for _, fc := range mc.Changes {
				sb.WriteString(fmt.Sprintf("        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New))
			}
		}
	}

	if len(mt.AddedConstraints) > 0 {
		sb.WriteString("    Added constraints:\n")
		for _, c := range mt.AddedConstraints {
			sb.WriteString(fmt.Sprintf("      - %s (%s)\n", c.Name, c.Type))
		}
	}

	if len(mt.RemovedConstraints) > 0 {
		sb.WriteString("    Removed constraints:\n")
		for _, c := range mt.RemovedConstraints {
			sb.WriteString(fmt.Sprintf("      - %s (%s)\n", c.Name, c.Type))
		}
	}

	if len(mt.ModifiedConstraints) > 0 {
		sb.WriteString("    Modified constraints:\n")
		for _, mc := range mt.ModifiedConstraints {
			if mc == nil {
				continue
			}
			name := mc.Name
			if name == "" {
				switch {
				case mc.New != nil:
					name = string(mc.New.Type)
				case mc.Old != nil:
					name = string(mc.Old.Type)
				default:
					name = "(unnamed)"
				}
			}
			sb.WriteString(fmt.Sprintf("      - %s:\n", name))
			for _, fc := range mc.Changes {
				sb.WriteString(fmt.Sprintf("        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New))
			}
		}
	}

	if len(mt.AddedIndexes) > 0 {
		sb.WriteString("    Added indexes:\n")
		for _, idx := range mt.AddedIndexes {
			sb.WriteString(fmt.Sprintf("      - %s %s\n", idx.Name, formatIndexColumns(idx.Columns)))
		}
	}

	if len(mt.RemovedIndexes) > 0 {
		sb.WriteString("    Removed indexes:\n")
		for _, idx := range mt.RemovedIndexes {
			sb.WriteString(fmt.Sprintf("      - %s %s\n", idx.Name, formatIndexColumns(idx.Columns)))
		}
	}

	if len(mt.ModifiedIndexes) > 0 {
		sb.WriteString("    Modified indexes:\n")
		for _, mi := range mt.ModifiedIndexes {
			name := mi.Name
			if name == "" {
				name = "(unnamed)"
			}
			sb.WriteString(fmt.Sprintf("      - %s:\n", name))
			for _, fc := range mi.Changes {
				sb.WriteString(fmt.Sprintf("        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New))
			}
		}
	}
}

func (d *SchemaDiff) IsEmpty() bool {
	return len(d.AddedTables) == 0 && len(d.RemovedTables) == 0 && len(d.ModifiedTables) == 0
}

// SaveToFile function save a SchemaDiff struct to a file of a given path.
// 0644 permissions means read/write for owner, read for group and others.
func (d *SchemaDiff) SaveToFile(path string) error {
	return os.WriteFile(path, []byte(d.String()), 0644)
}
