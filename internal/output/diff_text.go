package output

import (
	"fmt"
	"strings"

	"smf/internal/core"
	"smf/internal/diff"
)

// formatDiffText returns a string representation of all schema differences between two SQL dumps.
func formatDiffText(d *diff.SchemaDiff) string {
	if d.IsEmpty() {
		return "No differences detected."
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Schema differences:\n")

	writeDiffWarnings(&sb, d.Warnings)
	writeAddedTables(&sb, d.AddedTables)
	writeRemovedTables(&sb, d.RemovedTables)
	writeModifiedTables(&sb, d.ModifiedTables)

	return sb.String()
}

func writeDiffWarnings(sb *strings.Builder, warnings []string) {
	if len(warnings) > 0 {
		fmt.Fprintf(sb, "\nWarnings:\n")
		for _, w := range warnings {
			w = strings.TrimSpace(w)
			if w == "" {
				continue
			}
			fmt.Fprintf(sb, "  - %s\n", w)
		}
	}
}

func writeAddedTables(sb *strings.Builder, tables []*core.Table) {
	if len(tables) > 0 {
		fmt.Fprintf(sb, "\nAdded tables:\n")
		for _, at := range tables {
			fmt.Fprintf(sb, "  - %s\n", at.Name)
		}
	}
}

func writeRemovedTables(sb *strings.Builder, tables []*core.Table) {
	if len(tables) > 0 {
		fmt.Fprintf(sb, "\nRemoved tables:\n")
		for _, rt := range tables {
			fmt.Fprintf(sb, "  - %s\n", rt.Name)
		}
	}
}

func writeModifiedTables(sb *strings.Builder, tables []*diff.TableDiff) {
	if len(tables) > 0 {
		fmt.Fprintf(sb, "\nModified tables:\n")
		for _, mt := range tables {
			writeTableDiffText(sb, mt)
		}
	}
}

func writeTableDiffText(sb *strings.Builder, mt *diff.TableDiff) {
	fmt.Fprintf(sb, "\n  - %s\n", mt.Name)

	writeTableWarnings(sb, mt.Warnings)
	writeModifiedOptions(sb, mt.ModifiedOptions)
	writeColumns(sb, mt)
	writeConstraints(sb, mt)
	writeIndexes(sb, mt)
}

func writeTableWarnings(sb *strings.Builder, warnings []string) {
	if len(warnings) > 0 {
		fmt.Fprintf(sb, "    Warnings:\n")
		for _, w := range warnings {
			w = strings.TrimSpace(w)
			if w == "" {
				continue
			}
			fmt.Fprintf(sb, "      - %s\n", w)
		}
	}
}

func writeModifiedOptions(sb *strings.Builder, options []*diff.TableOptionChange) {
	if len(options) > 0 {
		fmt.Fprintf(sb, "    Options changed:\n")
		for _, mo := range options {
			fmt.Fprintf(sb, "      - %s: %q -> %q\n", mo.Name, mo.Old, mo.New)
		}
	}
}

func writeColumns(sb *strings.Builder, mt *diff.TableDiff) {
	if len(mt.AddedColumns) > 0 {
		fmt.Fprintf(sb, "    Added columns:\n")
		for _, ac := range mt.AddedColumns {
			fmt.Fprintf(sb, "      - %s: %s\n", ac.Name, ac.TypeRaw)
		}
	}

	if len(mt.RemovedColumns) > 0 {
		fmt.Fprintf(sb, "    Removed columns:\n")
		for _, rc := range mt.RemovedColumns {
			fmt.Fprintf(sb, "      - %s: %s\n", rc.Name, rc.TypeRaw)
		}
	}

	if len(mt.ModifiedColumns) > 0 {
		fmt.Fprintf(sb, "    Modified columns:\n")
		for _, mc := range mt.ModifiedColumns {
			fmt.Fprintf(sb, "      - %s:\n", mc.Name)
			for _, fc := range mc.Changes {
				fmt.Fprintf(sb, "        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New)
			}
		}
	}
}

func writeConstraints(sb *strings.Builder, mt *diff.TableDiff) {
	if len(mt.AddedConstraints) > 0 {
		fmt.Fprintf(sb, "    Added constraints:\n")
		for _, c := range mt.AddedConstraints {
			fmt.Fprintf(sb, "      - %s (%s)\n", c.Name, c.Type)
		}
	}

	if len(mt.RemovedConstraints) > 0 {
		fmt.Fprintf(sb, "    Removed constraints:\n")
		for _, c := range mt.RemovedConstraints {
			fmt.Fprintf(sb, "      - %s (%s)\n", c.Name, c.Type)
		}
	}

	if len(mt.ModifiedConstraints) > 0 {
		fmt.Fprintf(sb, "    Modified constraints:\n")
		for _, mc := range mt.ModifiedConstraints {
			if mc == nil {
				continue
			}
			name := getConstraintName(mc)
			fmt.Fprintf(sb, "      - %s:\n", name)
			for _, fc := range mc.Changes {
				fmt.Fprintf(sb, "        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New)
			}
		}
	}
}

func getConstraintName(mc *diff.ConstraintChange) string {
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
	return name
}

func writeIndexes(sb *strings.Builder, mt *diff.TableDiff) {
	if len(mt.AddedIndexes) > 0 {
		fmt.Fprintf(sb, "    Added indexes:\n")
		for _, idx := range mt.AddedIndexes {
			fmt.Fprintf(sb, "      - %s %s\n", idx.Name, diff.FormatIndexColumns(idx.Columns))
		}
	}

	if len(mt.RemovedIndexes) > 0 {
		fmt.Fprintf(sb, "    Removed indexes:\n")
		for _, idx := range mt.RemovedIndexes {
			fmt.Fprintf(sb, "      - %s %s\n", idx.Name, diff.FormatIndexColumns(idx.Columns))
		}
	}

	if len(mt.ModifiedIndexes) > 0 {
		fmt.Fprintf(sb, "    Modified indexes:\n")
		for _, mi := range mt.ModifiedIndexes {
			name := mi.Name
			if name == "" {
				name = "(unnamed)"
			}
			fmt.Fprintf(sb, "      - %s:\n", name)
			for _, fc := range mi.Changes {
				fmt.Fprintf(sb, "        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New)
			}
		}
	}
}
