package diff

import (
	"strings"

	"smf/internal/core"
)

func (td *TableDiff) detectColumnRenames() {
	if len(td.RemovedColumns) == 0 || len(td.AddedColumns) == 0 {
		return
	}

	usedAdded := make(map[int]struct{}, len(td.AddedColumns))
	maxRenames := max(len(td.AddedColumns), len(td.RemovedColumns))

	renames := make([]*ColumnRename, 0, maxRenames)

	removedTokens := make([][]string, len(td.RemovedColumns))
	for i, c := range td.RemovedColumns {
		removedTokens[i] = tokenizeName(c.Name)
	}
	addedTokens := make([][]string, len(td.AddedColumns))
	for i, c := range td.AddedColumns {
		addedTokens[i] = tokenizeName(c.Name)
	}

	for i, oldC := range td.RemovedColumns {
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
			if !renameEvidenceWithTokens(oldC, newC, removedTokens[i], addedTokens[bestIdx]) {
				continue
			}
			if !strings.EqualFold(oldC.TypeRaw, newC.TypeRaw) {
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
	return compareColumnAttrs(oldC, newC).similarityScore()
}

func renameEvidenceWithTokens(oldC, newC *core.Column, oldTokens, newTokens []string) bool {
	if hasSharedTokens(oldTokens, newTokens) {
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

// tokenizeName splits a column name into lowercase tokens for comparison.
func tokenizeName(name string) []string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return nil
	}
	f := func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	}
	parts := strings.FieldsFunc(name, f)
	var out []string
	for _, p := range parts {
		if len(p) >= renameSharedTokenMinLen {
			out = append(out, p)
		}
	}
	return out
}

// hasSharedTokens checks if two token slices share any common token.
func hasSharedTokens(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, t := range a {
		set[t] = struct{}{}
	}
	for _, t := range b {
		if _, ok := set[t]; ok {
			return true
		}
	}
	return false
}

// hasSharedNameToken checks if two names share a common token (for non-cached usage).
func hasSharedNameToken(a, b string) bool {
	return hasSharedTokens(tokenizeName(a), tokenizeName(b))
}
