package output

import (
	"fmt"
	"strings"

	"smf/internal/diff"
)

// formatDiffText returns a string representation of all schema differences between two SQL dumps.
func formatDiffText(d *diff.SchemaDiff) string {
	if d.IsEmpty() {
		return "No differences detected."
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Schema differences:\n")

	if len(d.Warnings) > 0 {
		fmt.Fprintf(&sb, "\nWarnings:\n")
		for _, w := range d.Warnings {
			w = strings.TrimSpace(w)
			if w == "" {
				continue
			}
			fmt.Fprintf(&sb, "  - %s\n", w)
		}
	}

	if len(d.AddedTables) > 0 {
		fmt.Fprintf(&sb, "\nAdded tables:\n")
		for _, at := range d.AddedTables {
			fmt.Fprintf(&sb, "  - %s\n", at.Name)
		}
	}

	if len(d.RemovedTables) > 0 {
		fmt.Fprintf(&sb, "\nRemoved tables:\n")
		for _, rt := range d.RemovedTables {
			fmt.Fprintf(&sb, "  - %s\n", rt.Name)
		}
	}

	if len(d.ModifiedTables) > 0 {
		fmt.Fprintf(&sb, "\nModified tables:\n")
		for _, mt := range d.ModifiedTables {
			writeTableDiffText(&sb, mt)
		}
	}

	return sb.String()
}

func writeTableDiffText(sb *strings.Builder, mt *diff.TableDiff) {
	fmt.Fprintf(sb, "\n  - %s\n", mt.Name)

	if len(mt.Warnings) > 0 {
		fmt.Fprintf(sb, "    Warnings:\n")
		for _, w := range mt.Warnings {
			w = strings.TrimSpace(w)
			if w == "" {
				continue
			}
			fmt.Fprintf(sb, "      - %s\n", w)
		}
	}

	if len(mt.ModifiedOptions) > 0 {
		fmt.Fprintf(sb, "    Options changed:\n")
		for _, mo := range mt.ModifiedOptions {
			fmt.Fprintf(sb, "      - %s: %q -> %q\n", mo.Name, mo.Old, mo.New)
		}
	}

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
			fmt.Fprintf(sb, "      - %s:\n", name)
			for _, fc := range mc.Changes {
				fmt.Fprintf(sb, "        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New)
			}
		}
	}

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
