package diff

import (
	"strings"

	"smf/internal/core"
)

func compareConstraints(oldItems, newItems []*core.Constraint, td *TableDiff) {
	oldMap := mapConstraintsByKey(oldItems, constraintKey)
	newMap := mapConstraintsByKey(newItems, constraintKey)

	for name, newItem := range newMap {
		oldItem, exists := oldMap[name]
		if !exists {
			td.AddedConstraints = append(td.AddedConstraints, newItem)
			continue
		}
		if !equalConstraint(oldItem, newItem) {
			td.ModifiedConstraints = append(td.ModifiedConstraints, &ConstraintChange{
				Name:    newItem.Name,
				Old:     oldItem,
				New:     newItem,
				Changes: constraintFieldChanges(oldItem, newItem),
			})
		}
	}

	for name, oldItem := range oldMap {
		if _, exists := newMap[name]; !exists {
			td.RemovedConstraints = append(td.RemovedConstraints, oldItem)
		}
	}
}

func markConstraintsForRebuild(oldItems, newItems []*core.Constraint, td *TableDiff) {
	if len(td.ModifiedColumns) == 0 {
		return
	}

	affectedCols := make(map[string]struct{}, len(td.ModifiedColumns))
	for _, mc := range td.ModifiedColumns {
		if mc == nil {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(mc.Name))
		if name == "" {
			continue
		}
		affectedCols[name] = struct{}{}
	}
	if len(affectedCols) == 0 {
		return
	}

	oldMap := mapConstraintsByKey(oldItems, constraintKey)
	newMap := mapConstraintsByKey(newItems, constraintKey)

	already := make(map[string]struct{}, len(td.ModifiedConstraints))
	for _, mc := range td.ModifiedConstraints {
		if mc == nil {
			continue
		}
		if mc.New != nil {
			already[constraintKey(mc.New)] = struct{}{}
		} else if mc.Old != nil {
			already[constraintKey(mc.Old)] = struct{}{}
		}
	}

	for key, oldC := range oldMap {
		newC, ok := newMap[key]
		if !ok {
			continue
		}
		if _, ok := already[key]; ok {
			continue
		}
		if !equalConstraint(oldC, newC) {
			continue
		}
		if !constraintTouchesColumns(newC, affectedCols) {
			continue
		}
		td.ModifiedConstraints = append(td.ModifiedConstraints, &ConstraintChange{
			Name:          newC.Name,
			Old:           oldC,
			New:           newC,
			Changes:       nil,
			RebuildOnly:   true,
			RebuildReason: "dependent column modified",
		})
	}
}

func constraintTouchesColumns(c *core.Constraint, cols map[string]struct{}) bool {
	if c == nil || len(cols) == 0 {
		return false
	}
	for _, col := range c.Columns {
		name := strings.ToLower(strings.TrimSpace(col))
		if name == "" {
			continue
		}
		if _, ok := cols[name]; ok {
			return true
		}
	}
	return false
}

func equalConstraint(a, b *core.Constraint) bool {
	if a.Type != b.Type {
		return false
	}
	if !equalStringSliceCI(a.Columns, b.Columns) {
		return false
	}
	if !strings.EqualFold(a.ReferencedTable, b.ReferencedTable) {
		return false
	}
	if !equalStringSliceCI(a.ReferencedColumns, b.ReferencedColumns) {
		return false
	}
	if a.OnDelete != b.OnDelete {
		return false
	}
	if a.OnUpdate != b.OnUpdate {
		return false
	}
	if strings.TrimSpace(a.CheckExpression) != strings.TrimSpace(b.CheckExpression) {
		return false
	}
	return true
}

func constraintKey(c *core.Constraint) string {
	name := strings.ToLower(strings.TrimSpace(c.Name))
	if name != "" {
		return name
	}
	var sb strings.Builder
	sb.Grow(32)
	sb.WriteString(strings.ToLower(string(c.Type)))
	sb.WriteByte(':')
	for i, col := range c.Columns {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strings.ToLower(col))
	}
	return sb.String()
}

func constraintFieldChanges(oldC, newC *core.Constraint) []*FieldChange {
	c := &fieldChangeCollector{}

	c.Add("type", string(oldC.Type), string(newC.Type))
	c.Add("columns", formatNameList(oldC.Columns), formatNameList(newC.Columns))
	c.Add("referenced_table", oldC.ReferencedTable, newC.ReferencedTable)
	c.Add("referenced_columns", formatNameList(oldC.ReferencedColumns), formatNameList(newC.ReferencedColumns))
	c.Add("on_delete", string(oldC.OnDelete), string(newC.OnDelete))
	c.Add("on_update", string(oldC.OnUpdate), string(newC.OnUpdate))
	c.Add("check_expression", strings.TrimSpace(oldC.CheckExpression), strings.TrimSpace(newC.CheckExpression))

	return c.Changes
}
