package diff

import (
	"schemift/core"
	"strings"
)

func (td *TableDiff) detectColumnRenames() {
	if td == nil || len(td.RemovedColumns) == 0 || len(td.AddedColumns) == 0 {
		return
	}

	usedAdded := make(map[int]struct{}, len(td.AddedColumns))
	var renames []*ColumnRename

	for _, oldC := range td.RemovedColumns {
		if oldC == nil {
			continue
		}
		bestIdx := -1
		bestScore := -1
		for j, newC := range td.AddedColumns {
			if newC == nil {
				continue
			}
			if _, ok := usedAdded[j]; ok {
				continue
			}
			score := renameSimilarityScore(oldC, newC)
			if score > bestScore {
				bestScore = score
				bestIdx = j
			}
		}
		if bestIdx >= 0 && bestScore >= renameDetectionScoreThreshold {
			newC := td.AddedColumns[bestIdx]
			if !renameEvidence(oldC, newC) {
				continue
			}
			usedAdded[bestIdx] = struct{}{}
			renames = append(renames, &ColumnRename{Old: oldC, New: newC, Score: bestScore})
		}
	}

	if len(renames) == 0 {
		return
	}

	removeOld := make(map[*core.Column]struct{}, len(renames))
	removeNew := make(map[*core.Column]struct{}, len(renames))
	for _, r := range renames {
		if r == nil {
			continue
		}
		removeOld[r.Old] = struct{}{}
		removeNew[r.New] = struct{}{}
	}

	var keptRemoved []*core.Column
	for _, c := range td.RemovedColumns {
		if c == nil {
			continue
		}
		if _, ok := removeOld[c]; ok {
			continue
		}
		keptRemoved = append(keptRemoved, c)
	}

	var keptAdded []*core.Column
	for _, c := range td.AddedColumns {
		if c == nil {
			continue
		}
		if _, ok := removeNew[c]; ok {
			continue
		}
		keptAdded = append(keptAdded, c)
	}

	td.RemovedColumns = keptRemoved
	td.AddedColumns = keptAdded
	td.RenamedColumns = append(td.RenamedColumns, renames...)
}

func renameSimilarityScore(oldC, newC *core.Column) int {
	if oldC == nil || newC == nil {
		return 0
	}
	if strings.EqualFold(oldC.Name, newC.Name) {
		return 0
	}
	score := 0
	if strings.EqualFold(oldC.TypeRaw, newC.TypeRaw) {
		score += 4
	}
	if oldC.Type == newC.Type {
		score += 2
	}
	if oldC.Nullable == newC.Nullable {
		score += 1
	}
	if oldC.AutoIncrement == newC.AutoIncrement {
		score += 1
	}
	if oldC.PrimaryKey == newC.PrimaryKey {
		score += 1
	}
	if PtrEqString(oldC.DefaultValue, newC.DefaultValue) {
		score += 1
	}
	if strings.EqualFold(strings.TrimSpace(oldC.Charset), strings.TrimSpace(newC.Charset)) {
		score += 1
	}
	if strings.EqualFold(strings.TrimSpace(oldC.Collate), strings.TrimSpace(newC.Collate)) {
		score += 1
	}
	if oldC.IsGenerated == newC.IsGenerated {
		score += 1
	}
	if strings.TrimSpace(oldC.GenerationExpression) == strings.TrimSpace(newC.GenerationExpression) {
		score += 1
	}
	if strings.EqualFold(string(oldC.GenerationStorage), string(newC.GenerationStorage)) {
		score += 1
	}
	if strings.EqualFold(oldC.Comment, newC.Comment) {
		score += 1
	}

	return score
}

func renameEvidence(oldC, newC *core.Column) bool {
	if oldC == nil || newC == nil {
		return false
	}
	if hasSharedNameToken(oldC.Name, newC.Name) {
		return true
	}
	if strings.TrimSpace(oldC.Comment) != "" && strings.EqualFold(strings.TrimSpace(oldC.Comment), strings.TrimSpace(newC.Comment)) {
		return true
	}
	if oldC.IsGenerated && newC.IsGenerated {
		oldExpr := strings.TrimSpace(oldC.GenerationExpression)
		newExpr := strings.TrimSpace(newC.GenerationExpression)
		if oldExpr != "" && oldExpr == newExpr {
			return true
		}
	}
	return false
}

func hasSharedNameToken(a, b string) bool {
	a = strings.ToLower(strings.TrimSpace(a))
	b = strings.ToLower(strings.TrimSpace(b))
	if a == "" || b == "" {
		return false
	}

	split := func(s string) []string {
		f := func(r rune) bool {
			return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
		}
		parts := strings.FieldsFunc(s, f)
		var out []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if len(p) < renameSharedTokenMinLen {
				continue
			}
			out = append(out, p)
		}
		return out
	}

	ta := split(a)
	tb := split(b)
	if len(ta) == 0 || len(tb) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(ta))
	for _, t := range ta {
		set[t] = struct{}{}
	}
	for _, t := range tb {
		if _, ok := set[t]; ok {
			return true
		}
	}
	return false
}
