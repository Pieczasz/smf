package diff

import (
	"strconv"
	"strings"

	"smf/internal/core"
)

const (
	// OptionsCount is the number of options that we support for MySQL dialect.
	OptionsCount = 45
)

func compareTable(oldT, newT *core.Table, opts Options) *TableDiff {
	td := &TableDiff{Name: newT.Name}

	compareColumns(oldT.Columns, newT.Columns, td, opts)
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

func compareColumns(oldItems, newItems []*core.Column, td *TableDiff, opts Options) {
	oldMap, oldCollisions := mapColumnsByName(oldItems)
	newMap, newCollisions := mapColumnsByName(newItems)
	for _, c := range oldCollisions {
		td.Warnings = append(td.Warnings, "old table columns: "+c)
	}
	for _, c := range newCollisions {
		td.Warnings = append(td.Warnings, "new table columns: "+c)
	}

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

	if opts.DetectColumnRenames {
		td.detectColumnRenames()
	}
}

func equalColumn(a, b *core.Column) bool {
	return compareColumnAttrs(a, b).allMatch()
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
	c.Add("auto_random", strconv.FormatUint(oldC.AutoRandom, 10), strconv.FormatUint(newC.AutoRandom, 10))

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

func tableOptionMap(t *core.Table) map[string]string {
	o := t.Options
	m := make(map[string]string, OptionsCount)

	addStr := func(name, val string) {
		if v := strings.TrimSpace(val); v != "" {
			m[name] = v
		}
	}

	addU64 := func(name string, val uint64) {
		if val != 0 {
			m[name] = strconv.FormatUint(val, 10)
		}
	}

	addBool := func(name string, val bool) {
		if val {
			m[name] = "ON"
		}
	}

	addStr("AUTOEXTEND_SIZE", o.AutoextendSize)
	addU64("AUTO_INCREMENT", o.AutoIncrement)
	addU64("AVG_ROW_LENGTH", o.AvgRowLength)
	addStr("CHARSET", o.Charset)
	addU64("CHECKSUM", o.Checksum)
	addStr("COLLATE", o.Collate)
	addStr("COMMENT", t.Comment)
	addStr("COMPRESSION", o.Compression)
	addStr("CONNECTION", o.Connection)
	addStr("DATA DIRECTORY", o.DataDirectory)
	addU64("DELAY_KEY_WRITE", o.DelayKeyWrite)
	addStr("ENCRYPTION", o.Encryption)
	addStr("ENGINE", o.Engine)
	addStr("INDEX DIRECTORY", o.IndexDirectory)
	addStr("INSERT_METHOD", o.InsertMethod)
	addU64("KEY_BLOCK_SIZE", o.KeyBlockSize)
	addU64("MAX_ROWS", o.MaxRows)
	addU64("MIN_ROWS", o.MinRows)
	addStr("PACK_KEYS", o.PackKeys)
	addU64("PAGE_CHECKSUM", o.PageChecksum)
	addStr("PASSWORD", o.Password)
	addStr("ROW_FORMAT", o.RowFormat)
	addStr("STATS_AUTO_RECALC", o.StatsAutoRecalc)
	addStr("STATS_PERSISTENT", o.StatsPersistent)
	addStr("STATS_SAMPLE_PAGES", o.StatsSamplePages)
	addStr("STORAGE_MEDIA", o.StorageMedia)
	addStr("TABLESPACE", o.Tablespace)
	addU64("TRANSACTIONAL", o.Transactional)

	addStr("SECONDARY_ENGINE", o.MySQL.SecondaryEngine)
	addU64("TABLE_CHECKSUM", o.MySQL.TableChecksum)
	addStr("ENGINE_ATTRIBUTE", o.MySQL.EngineAttribute)
	addStr("SECONDARY_ENGINE_ATTRIBUTE", o.MySQL.SecondaryEngineAttribute)
	addBool("PAGE_COMPRESSED", o.MySQL.PageCompressed)
	addU64("PAGE_COMPRESSION_LEVEL", o.MySQL.PageCompressionLevel)
	addBool("IETF_QUOTES", o.MySQL.IetfQuotes)
	addU64("NODEGROUP", o.MySQL.Nodegroup)
	if len(o.MySQL.Union) > 0 {
		m["UNION"] = strings.Join(o.MySQL.Union, ",")
	}

	addU64("AUTO_ID_CACHE", o.TiDB.AutoIDCache)
	addU64("AUTO_RANDOM_BASE", o.TiDB.AutoRandomBase)
	addU64("SHARD_ROW_ID_BITS", o.TiDB.ShardRowID)
	addU64("PRE_SPLIT_REGIONS", o.TiDB.PreSplitRegion)
	addStr("TTL", o.TiDB.TTL)
	addBool("TTL_ENABLE", o.TiDB.TTLEnable)
	addStr("TTL_JOB_INTERVAL", o.TiDB.TTLJobInterval)
	addStr("PLACEMENT_POLICY", o.TiDB.PlacementPolicy)

	return m
}

func (td *TableDiff) sort() {
	sortNamed(td.AddedColumns)
	sortNamed(td.RemovedColumns)
	// ColumnRename needs special handling - it uses New.Name, not a direct Name field
	sortByFunc(td.RenamedColumns, func(r *ColumnRename) string {
		if r == nil || r.New == nil {
			return ""
		}
		return r.New.Name
	})
	sortNamed(td.ModifiedColumns)
	sortNamed(td.AddedConstraints)
	sortNamed(td.RemovedConstraints)
	sortNamed(td.ModifiedConstraints)
	sortNamed(td.AddedIndexes)
	sortNamed(td.RemovedIndexes)
	sortNamed(td.ModifiedIndexes)
	sortNamed(td.ModifiedOptions)
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
