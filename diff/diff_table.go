package diff

import (
	"fmt"
	"schemift/core"
	"sort"
	"strconv"
	"strings"
)

func compareTable(oldT, newT *core.Table) *TableDiff {
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

func compareColumns(oldItems, newItems []*core.Column, td *TableDiff) {
	oldMap := mapByLowerName(oldItems, func(c *core.Column) string { return c.Name })
	newMap := mapByLowerName(newItems, func(c *core.Column) string { return c.Name })

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

func equalColumn(a, b *core.Column) bool {
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

func columnFieldChanges(oldC, newC *core.Column) []*FieldChange {
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

func compareOptions(oldT, newT *core.Table, td *TableDiff) {
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

func tableOptionMap(t *core.Table) map[string]string {
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

func (td *TableDiff) sort() {
	sortByNameCI(td.AddedColumns, func(c *core.Column) string { return c.Name })
	sortByNameCI(td.RemovedColumns, func(c *core.Column) string { return c.Name })
	sortByNameCI(td.RenamedColumns, func(r *ColumnRename) string {
		if r == nil || r.New == nil {
			return ""
		}
		return r.New.Name
	})
	sortByNameCI(td.ModifiedColumns, func(ch *ColumnChange) string { return ch.Name })
	sortByNameCI(td.AddedConstraints, func(c *core.Constraint) string { return c.Name })
	sortByNameCI(td.RemovedConstraints, func(c *core.Constraint) string { return c.Name })
	sortByNameCI(td.ModifiedConstraints, func(ch *ConstraintChange) string { return ch.Name })
	sortByNameCI(td.AddedIndexes, func(i *core.Index) string { return i.Name })
	sortByNameCI(td.RemovedIndexes, func(i *core.Index) string { return i.Name })
	sortByNameCI(td.ModifiedIndexes, func(ch *IndexChange) string { return ch.Name })
	sortByNameCI(td.ModifiedOptions, func(o *TableOptionChange) string { return o.Name })
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
