package core

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	renameDetectionScoreThreshold = 9
	renameSharedTokenMinLen       = 3
)

type SchemaDiff struct {
	AddedTables    []*Table
	RemovedTables  []*Table
	ModifiedTables []*TableDiff
}

type TableDiff struct {
	Name                string
	AddedColumns        []*Column
	RemovedColumns      []*Column
	RenamedColumns      []*ColumnRename
	ModifiedColumns     []*ColumnChange
	AddedConstraints    []*Constraint
	RemovedConstraints  []*Constraint
	ModifiedConstraints []*ConstraintChange
	AddedIndexes        []*Index
	RemovedIndexes      []*Index
	ModifiedIndexes     []*IndexChange
	ModifiedOptions     []*TableOptionChange
}

type ColumnChange struct {
	Name    string
	Old     *Column
	New     *Column
	Changes []*FieldChange
}

type ColumnRename struct {
	Old   *Column
	New   *Column
	Score int
}

type ConstraintChange struct {
	Name          string
	Old           *Constraint
	New           *Constraint
	Changes       []*FieldChange
	RebuildOnly   bool
	RebuildReason string
}

type IndexChange struct {
	Name    string
	Old     *Index
	New     *Index
	Changes []*FieldChange
}

type FieldChange struct {
	Field string
	Old   string
	New   string
}

type TableOptionChange struct {
	Name string
	Old  string
	New  string
}

func Diff(oldDB, newDB *Database) *SchemaDiff {
	d := &SchemaDiff{}

	oldTables := mapByLowerName(oldDB.Tables, func(t *Table) string { return t.Name })
	newTables := mapByLowerName(newDB.Tables, func(t *Table) string { return t.Name })

	for name, nt := range newTables {
		ot, ok := oldTables[name]
		if !ok {
			d.AddedTables = append(d.AddedTables, nt)
			continue
		}

		td := compareTable(ot, nt)
		if td != nil {
			d.ModifiedTables = append(d.ModifiedTables, td)
		}
	}

	for name, ot := range oldTables {
		if _, ok := newTables[name]; !ok {
			d.RemovedTables = append(d.RemovedTables, ot)
		}
	}

	sortByNameCI(d.AddedTables, func(t *Table) string { return t.Name })
	sortByNameCI(d.RemovedTables, func(t *Table) string { return t.Name })
	sortByNameCI(d.ModifiedTables, func(td *TableDiff) string { return td.Name })

	return d
}

func compareTable(oldT, newT *Table) *TableDiff {
	td := &TableDiff{Name: newT.Name}

	compareColumns(oldT.Columns, newT.Columns, td)
	compareConstraints(oldT.Constraints, newT.Constraints, td)
	markConstraintsForRebuild(oldT.Constraints, newT.Constraints, td)
	compareIndexes(oldT.Indexes, newT.Indexes, td)
	compareOptions(oldT, newT, td)

	if td.isEmpty() {
		return nil
	}

	td.sort()
	return td
}

func markConstraintsForRebuild(oldItems, newItems []*Constraint, td *TableDiff) {
	if td == nil {
		return
	}
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

	oldMap := mapByKey(oldItems, constraintKey)
	newMap := mapByKey(newItems, constraintKey)

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

func constraintTouchesColumns(c *Constraint, cols map[string]struct{}) bool {
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

func compareColumns(oldItems, newItems []*Column, td *TableDiff) {
	oldMap := mapByLowerName(oldItems, func(c *Column) string { return c.Name })
	newMap := mapByLowerName(newItems, func(c *Column) string { return c.Name })

	for name, newItem := range newMap {
		oldItem, exists := oldMap[name]
		if !exists {
			td.AddedColumns = append(td.AddedColumns, newItem)
			continue
		}
		if !equalColumn(oldItem, newItem) {
			td.ModifiedColumns = append(td.ModifiedColumns, &ColumnChange{
				Name:    newItem.Name,
				Old:     oldItem,
				New:     newItem,
				Changes: columnFieldChanges(oldItem, newItem),
			})
		}
	}

	for name, oldItem := range oldMap {
		if _, exists := newMap[name]; !exists {
			td.RemovedColumns = append(td.RemovedColumns, oldItem)
		}
	}

	td.detectColumnRenames()
}

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

	removeOld := make(map[*Column]struct{}, len(renames))
	removeNew := make(map[*Column]struct{}, len(renames))
	for _, r := range renames {
		if r == nil {
			continue
		}
		removeOld[r.Old] = struct{}{}
		removeNew[r.New] = struct{}{}
	}

	var keptRemoved []*Column
	for _, c := range td.RemovedColumns {
		if c == nil {
			continue
		}
		if _, ok := removeOld[c]; ok {
			continue
		}
		keptRemoved = append(keptRemoved, c)
	}

	var keptAdded []*Column
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

func renameSimilarityScore(oldC, newC *Column) int {
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
	if ptrEqString(oldC.DefaultValue, newC.DefaultValue) {
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

func renameEvidence(oldC, newC *Column) bool {
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

func compareConstraints(oldItems, newItems []*Constraint, td *TableDiff) {
	oldMap := mapByKey(oldItems, constraintKey)
	newMap := mapByKey(newItems, constraintKey)

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

func compareIndexes(oldItems, newItems []*Index, td *TableDiff) {
	oldMap := mapByKey(oldItems, indexKey)
	newMap := mapByKey(newItems, indexKey)

	for name, newItem := range newMap {
		oldItem, exists := oldMap[name]
		if !exists {
			td.AddedIndexes = append(td.AddedIndexes, newItem)
			continue
		}
		if !equalIndex(oldItem, newItem) {
			td.ModifiedIndexes = append(td.ModifiedIndexes, &IndexChange{
				Name:    newItem.Name,
				Old:     oldItem,
				New:     newItem,
				Changes: indexFieldChanges(oldItem, newItem),
			})
		}
	}

	for name, oldItem := range oldMap {
		if _, exists := newMap[name]; !exists {
			td.RemovedIndexes = append(td.RemovedIndexes, oldItem)
		}
	}
}

func compareOptions(oldT, newT *Table, td *TableDiff) {
	oldOpt := tableOptionMap(oldT)
	newOpt := tableOptionMap(newT)
	for _, k := range unionKeys(oldOpt, newOpt) {
		ov, nv := oldOpt[k], newOpt[k]
		if ov == nv {
			continue
		}
		td.ModifiedOptions = append(td.ModifiedOptions, &TableOptionChange{Name: k, Old: ov, New: nv})
	}
}

func (td *TableDiff) isEmpty() bool {
	return len(td.AddedColumns) == 0 &&
		len(td.RemovedColumns) == 0 &&
		len(td.RenamedColumns) == 0 &&
		len(td.ModifiedColumns) == 0 &&
		len(td.AddedConstraints) == 0 &&
		len(td.RemovedConstraints) == 0 &&
		len(td.ModifiedConstraints) == 0 &&
		len(td.AddedIndexes) == 0 &&
		len(td.RemovedIndexes) == 0 &&
		len(td.ModifiedIndexes) == 0 &&
		len(td.ModifiedOptions) == 0
}

func (td *TableDiff) sort() {
	sortByNameCI(td.AddedColumns, func(c *Column) string { return c.Name })
	sortByNameCI(td.RemovedColumns, func(c *Column) string { return c.Name })
	sortByNameCI(td.RenamedColumns, func(r *ColumnRename) string {
		if r == nil || r.New == nil {
			return ""
		}
		return r.New.Name
	})
	sortByNameCI(td.ModifiedColumns, func(ch *ColumnChange) string { return ch.Name })
	sortByNameCI(td.AddedConstraints, func(c *Constraint) string { return c.Name })
	sortByNameCI(td.RemovedConstraints, func(c *Constraint) string { return c.Name })
	sortByNameCI(td.ModifiedConstraints, func(ch *ConstraintChange) string { return ch.Name })
	sortByNameCI(td.AddedIndexes, func(i *Index) string { return i.Name })
	sortByNameCI(td.RemovedIndexes, func(i *Index) string { return i.Name })
	sortByNameCI(td.ModifiedIndexes, func(ch *IndexChange) string { return ch.Name })
	sortByNameCI(td.ModifiedOptions, func(o *TableOptionChange) string { return o.Name })
}

func equalColumn(a, b *Column) bool {
	if !strings.EqualFold(a.TypeRaw, b.TypeRaw) {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	if a.PrimaryKey != b.PrimaryKey {
		return false
	}
	if a.AutoIncrement != b.AutoIncrement {
		return false
	}
	if !strings.EqualFold(a.Charset, b.Charset) {
		return false
	}
	if !strings.EqualFold(a.Collate, b.Collate) {
		return false
	}
	if a.Comment != b.Comment {
		return false
	}
	if ptrStr(a.DefaultValue) != ptrStr(b.DefaultValue) {
		return false
	}
	if ptrStr(a.OnUpdate) != ptrStr(b.OnUpdate) {
		return false
	}
	if a.IsGenerated != b.IsGenerated {
		return false
	}
	if strings.TrimSpace(a.GenerationExpression) != strings.TrimSpace(b.GenerationExpression) {
		return false
	}
	if !strings.EqualFold(string(a.GenerationStorage), string(b.GenerationStorage)) {
		return false
	}
	if !strings.EqualFold(a.ColumnFormat, b.ColumnFormat) {
		return false
	}
	if !strings.EqualFold(a.Storage, b.Storage) {
		return false
	}
	if a.AutoRandom != b.AutoRandom {
		return false
	}
	return true
}

func equalConstraint(a, b *Constraint) bool {
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

func equalIndex(a, b *Index) bool {
	if a.Unique != b.Unique {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !equalIndexColumns(a.Columns, b.Columns) {
		return false
	}
	if a.Comment != b.Comment {
		return false
	}
	if a.Visibility != b.Visibility {
		return false
	}
	return true
}

func equalIndexColumns(a, b []IndexColumn) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i].Name, b[i].Name) {
			return false
		}
		if a[i].Length != b[i].Length {
			return false
		}
		if a[i].Order != b[i].Order {
			return false
		}
	}
	return true
}

func equalStringSliceCI(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

func constraintKey(c *Constraint) string {
	name := strings.ToLower(strings.TrimSpace(c.Name))
	if name != "" {
		return name
	}
	return strings.ToLower(string(c.Type)) + ":" + strings.ToLower(strings.Join(c.Columns, ","))
}

func indexKey(i *Index) string {
	name := strings.ToLower(strings.TrimSpace(i.Name))
	if name != "" {
		return name
	}
	uniq := "0"
	if i.Unique {
		uniq = "1"
	}
	cols := make([]string, len(i.Columns))
	for idx, c := range i.Columns {
		cols[idx] = strings.ToLower(c.Name)
	}
	return "idx:" + uniq + ":" + strings.ToLower(string(i.Type)) + ":" + strings.Join(cols, ",")
}

func tableOptionMap(t *Table) map[string]string {
	o := t.Options
	m := map[string]string{
		"AUTOEXTEND_SIZE":    strings.TrimSpace(o.AutoextendSize),
		"AUTO_INCREMENT":     u64(o.AutoIncrement),
		"AVG_ROW_LENGTH":     u64(o.AvgRowLength),
		"CHARSET":            strings.TrimSpace(o.Charset),
		"CHECKSUM":           u64(o.Checksum),
		"COLLATE":            strings.TrimSpace(o.Collate),
		"COMMENT":            strings.TrimSpace(t.Comment),
		"COMPRESSION":        strings.TrimSpace(o.Compression),
		"CONNECTION":         strings.TrimSpace(o.Connection),
		"DATA DIRECTORY":     strings.TrimSpace(o.DataDirectory),
		"DELAY_KEY_WRITE":    u64(o.DelayKeyWrite),
		"ENCRYPTION":         strings.TrimSpace(o.Encryption),
		"ENGINE":             strings.TrimSpace(o.Engine),
		"INDEX DIRECTORY":    strings.TrimSpace(o.IndexDirectory),
		"INSERT_METHOD":      strings.TrimSpace(o.InsertMethod),
		"KEY_BLOCK_SIZE":     u64(o.KeyBlockSize),
		"MAX_ROWS":           u64(o.MaxRows),
		"MIN_ROWS":           u64(o.MinRows),
		"PACK_KEYS":          strings.TrimSpace(o.PackKeys),
		"PAGE_CHECKSUM":      u64(o.PageChecksum),
		"PASSWORD":           strings.TrimSpace(o.Password),
		"ROW_FORMAT":         strings.TrimSpace(o.RowFormat),
		"STATS_AUTO_RECALC":  strings.TrimSpace(o.StatsAutoRecalc),
		"STATS_PERSISTENT":   strings.TrimSpace(o.StatsPersistent),
		"STATS_SAMPLE_PAGES": strings.TrimSpace(o.StatsSamplePages),
		"STORAGE_MEDIA":      strings.TrimSpace(o.StorageMedia),
		"TABLESPACE":         strings.TrimSpace(o.Tablespace),
		"TRANSACTIONAL":      u64(o.Transactional),
	}

	if o.MySQL.SecondaryEngine != "" {
		m["SECONDARY_ENGINE"] = o.MySQL.SecondaryEngine
	}
	if o.MySQL.TableChecksum != 0 {
		m["TABLE_CHECKSUM"] = u64(o.MySQL.TableChecksum)
	}
	if o.MySQL.EngineAttribute != "" {
		m["ENGINE_ATTRIBUTE"] = o.MySQL.EngineAttribute
	}
	if o.MySQL.SecondaryEngineAttribute != "" {
		m["SECONDARY_ENGINE_ATTRIBUTE"] = o.MySQL.SecondaryEngineAttribute
	}
	if o.MySQL.PageCompressed {
		m["PAGE_COMPRESSED"] = "ON"
	}
	if o.MySQL.PageCompressionLevel != 0 {
		m["PAGE_COMPRESSION_LEVEL"] = u64(o.MySQL.PageCompressionLevel)
	}
	if o.MySQL.IetfQuotes {
		m["IETF_QUOTES"] = "ON"
	}
	if o.MySQL.Nodegroup != 0 {
		m["NODEGROUP"] = u64(o.MySQL.Nodegroup)
	}
	if len(o.MySQL.Union) > 0 {
		m["UNION"] = strings.Join(o.MySQL.Union, ",")
	}

	if o.TiDB.AutoIdCache != 0 {
		m["AUTO_ID_CACHE"] = u64(o.TiDB.AutoIdCache)
	}
	if o.TiDB.AutoRandomBase != 0 {
		m["AUTO_RANDOM_BASE"] = u64(o.TiDB.AutoRandomBase)
	}
	if o.TiDB.ShardRowID != 0 {
		m["SHARD_ROW_ID_BITS"] = u64(o.TiDB.ShardRowID)
	}
	if o.TiDB.PreSplitRegion != 0 {
		m["PRE_SPLIT_REGIONS"] = u64(o.TiDB.PreSplitRegion)
	}
	if o.TiDB.TTL != "" {
		m["TTL"] = o.TiDB.TTL
	}
	if o.TiDB.TTLEnable {
		m["TTL_ENABLE"] = "ON"
	}
	if o.TiDB.TTLJobInterval != "" {
		m["TTL_JOB_INTERVAL"] = o.TiDB.TTLJobInterval
	}
	if o.TiDB.PlacementPolicy != "" {
		m["PLACEMENT_POLICY"] = o.TiDB.PlacementPolicy
	}

	return m
}

func unionKeys(a, b map[string]string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return strings.ToLower(keys[i]) < strings.ToLower(keys[j])
	})
	return keys
}

func u64(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func columnFieldChanges(oldC, newC *Column) []*FieldChange {
	c := &fieldChangeCollector{}

	if !strings.EqualFold(oldC.TypeRaw, newC.TypeRaw) {
		c.Add("type", oldC.TypeRaw, newC.TypeRaw)
	}
	c.Add("nullable", strconv.FormatBool(oldC.Nullable), strconv.FormatBool(newC.Nullable))
	c.Add("primary_key", strconv.FormatBool(oldC.PrimaryKey), strconv.FormatBool(newC.PrimaryKey))
	c.Add("auto_increment", strconv.FormatBool(oldC.AutoIncrement), strconv.FormatBool(newC.AutoIncrement))
	c.Add("charset", strings.TrimSpace(oldC.Charset), strings.TrimSpace(newC.Charset))
	c.Add("collate", strings.TrimSpace(oldC.Collate), strings.TrimSpace(newC.Collate))
	c.Add("comment", oldC.Comment, newC.Comment)
	c.Add("default", ptrStr(oldC.DefaultValue), ptrStr(newC.DefaultValue))
	c.Add("on_update", ptrStr(oldC.OnUpdate), ptrStr(newC.OnUpdate))
	c.Add("generated", strconv.FormatBool(oldC.IsGenerated), strconv.FormatBool(newC.IsGenerated))
	c.Add("generation_expression", strings.TrimSpace(oldC.GenerationExpression), strings.TrimSpace(newC.GenerationExpression))
	c.Add("generation_storage", string(oldC.GenerationStorage), string(newC.GenerationStorage))
	c.Add("column_format", strings.TrimSpace(oldC.ColumnFormat), strings.TrimSpace(newC.ColumnFormat))
	c.Add("storage", strings.TrimSpace(oldC.Storage), strings.TrimSpace(newC.Storage))
	c.Add("auto_random", u64(oldC.AutoRandom), u64(newC.AutoRandom))

	return c.Changes
}

func constraintFieldChanges(oldC, newC *Constraint) []*FieldChange {
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

func indexFieldChanges(oldI, newI *Index) []*FieldChange {
	c := &fieldChangeCollector{}

	c.Add("unique", strconv.FormatBool(oldI.Unique), strconv.FormatBool(newI.Unique))
	c.Add("type", string(oldI.Type), string(newI.Type))
	c.Add("columns", formatIndexColumns(oldI.Columns), formatIndexColumns(newI.Columns))
	c.Add("comment", oldI.Comment, newI.Comment)
	c.Add("visibility", string(oldI.Visibility), string(newI.Visibility))

	return c.Changes
}

func formatIndexColumns(cols []IndexColumn) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return "(" + strings.Join(names, ", ") + ")"
}

type fieldChangeCollector struct {
	Changes []*FieldChange
}

func (c *fieldChangeCollector) Add(field, oldV, newV string) {
	if oldV == newV {
		return
	}
	c.Changes = append(c.Changes, &FieldChange{Field: field, Old: oldV, New: newV})
}

func sortByNameCI[T any](items []T, name func(T) string) {
	sort.Slice(items, func(i, j int) bool {
		return strings.ToLower(name(items[i])) < strings.ToLower(name(items[j]))
	})
}

func mapByLowerName[T any](items []T, name func(T) string) map[string]T {
	m := make(map[string]T, len(items))
	for _, item := range items {
		m[strings.ToLower(name(item))] = item
	}
	return m
}

func mapByKey[T any](items []T, key func(T) string) map[string]T {
	m := make(map[string]T, len(items))
	for _, item := range items {
		m[key(item)] = item
	}
	return m
}

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func formatNameList(items []string) string {
	return "(" + strings.Join(items, ", ") + ")"
}

func (d *SchemaDiff) IsEmpty() bool {
	return len(d.AddedTables) == 0 && len(d.RemovedTables) == 0 && len(d.ModifiedTables) == 0
}

func (d *SchemaDiff) String() string {
	if d.IsEmpty() {
		return "No differences detected."
	}

	var sb strings.Builder
	sb.WriteString("Schema differences:\n")

	if len(d.AddedTables) > 0 {
		sb.WriteString("\nAdded tables:\n")
		for _, t := range d.AddedTables {
			sb.WriteString(fmt.Sprintf("  - %s\n", t.Name))
		}
	}

	if len(d.RemovedTables) > 0 {
		sb.WriteString("\nRemoved tables:\n")
		for _, t := range d.RemovedTables {
			sb.WriteString(fmt.Sprintf("  - %s\n", t.Name))
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

	if len(mt.ModifiedOptions) > 0 {
		sb.WriteString("    Options changed:\n")
		for _, oc := range mt.ModifiedOptions {
			sb.WriteString(fmt.Sprintf("      - %s: %q -> %q\n", oc.Name, oc.Old, oc.New))
		}
	}

	if len(mt.AddedColumns) > 0 {
		sb.WriteString("    Added columns:\n")
		for _, c := range mt.AddedColumns {
			sb.WriteString(fmt.Sprintf("      - %s: %s\n", c.Name, c.TypeRaw))
		}
	}

	if len(mt.RemovedColumns) > 0 {
		sb.WriteString("    Removed columns:\n")
		for _, c := range mt.RemovedColumns {
			sb.WriteString(fmt.Sprintf("      - %s: %s\n", c.Name, c.TypeRaw))
		}
	}

	if len(mt.ModifiedColumns) > 0 {
		sb.WriteString("    Modified columns:\n")
		for _, ch := range mt.ModifiedColumns {
			sb.WriteString(fmt.Sprintf("      - %s:\n", ch.Name))
			for _, fc := range ch.Changes {
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
		for _, ch := range mt.ModifiedConstraints {
			if ch == nil {
				continue
			}
			name := ch.Name
			if name == "" {
				switch {
				case ch.New != nil:
					name = string(ch.New.Type)
				case ch.Old != nil:
					name = string(ch.Old.Type)
				default:
					name = "(unnamed)"
				}
			}
			sb.WriteString(fmt.Sprintf("      - %s:\n", name))
			for _, fc := range ch.Changes {
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
		for _, ch := range mt.ModifiedIndexes {
			name := ch.Name
			if name == "" {
				name = "(unnamed)"
			}
			sb.WriteString(fmt.Sprintf("      - %s:\n", name))
			for _, fc := range ch.Changes {
				sb.WriteString(fmt.Sprintf("        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New))
			}
		}
	}
}

func (d *SchemaDiff) SaveToFile(path string) error {
	return os.WriteFile(path, []byte(d.String()), 0644)
}
