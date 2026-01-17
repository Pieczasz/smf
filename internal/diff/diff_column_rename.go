package diff

import (
	"smf/internal/core"
	"strings"
)

func (td *TableDiff) detectColumnRenames() {
	if len(td.RemovedColumns) == 0 || len(td.AddedColumns) == 0 {
		return
	}

	usedAdded := make(map[int]struct{}, len(td.AddedColumns))
	var renames []*ColumnRename

	for _, oldC := range td.RemovedColumns {
		bestIdx := -1
		bestScore := -1
		for j, newC := range td.AddedColumns {
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
		removeOld[r.Old] = struct{}{}
		removeNew[r.New] = struct{}{}
	}

	var keptRemoved []*core.Column
	for _, c := range td.RemovedColumns {
		if _, ok := removeOld[c]; ok {
			continue
		}
		keptRemoved = append(keptRemoved, c)
	}

	var keptAdded []*core.Column
	for _, c := range td.AddedColumns {
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
	if strings.EqualFold(oldC.Name, newC.Name) {
		return 0
	}
	return compareColumnAttrs(oldC, newC).SimilarityScore()
}

func renameEvidence(oldC, newC *core.Column) bool {
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
