package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"

	"smf/internal/core"
)

func (p *Parser) parseTableOptions(opts []*ast.TableOption, table *core.Table) {
	for _, opt := range opts {
		if applyStandardTableOption(opt, table) {
			continue
		}
		if applyMySQLTableOption(opt, table) {
			continue
		}
		_ = p.applyTiDBTableOption(opt, table)
	}
}

// TODO: refactor this logic to remove nolint comment
//
//nolint:revive // Large switch needed for AST table option mapping
func applyStandardTableOption(opt *ast.TableOption, table *core.Table) bool {
	switch opt.Tp {
	case ast.TableOptionComment:
		table.Comment = opt.StrValue
	case ast.TableOptionCharset:
		table.Options.Charset = opt.StrValue
	case ast.TableOptionCollate:
		table.Options.Collate = opt.StrValue
	case ast.TableOptionEngine:
		table.Options.Engine = opt.StrValue
	case ast.TableOptionAutoIncrement:
		table.Options.AutoIncrement = opt.UintValue
	case ast.TableOptionAvgRowLength:
		table.Options.AvgRowLength = opt.UintValue
	case ast.TableOptionCheckSum:
		table.Options.Checksum = opt.UintValue
	case ast.TableOptionCompression:
		table.Options.Compression = opt.StrValue
	case ast.TableOptionKeyBlockSize:
		table.Options.KeyBlockSize = opt.UintValue
	case ast.TableOptionMaxRows:
		table.Options.MaxRows = opt.UintValue
	case ast.TableOptionMinRows:
		table.Options.MinRows = opt.UintValue
	case ast.TableOptionDelayKeyWrite:
		table.Options.DelayKeyWrite = opt.UintValue
	case ast.TableOptionRowFormat:
		table.Options.RowFormat = rowFormatToString(opt.UintValue)
	case ast.TableOptionTablespace:
		table.Options.Tablespace = opt.StrValue
	case ast.TableOptionDataDirectory:
		table.Options.DataDirectory = opt.StrValue
	case ast.TableOptionIndexDirectory:
		table.Options.IndexDirectory = opt.StrValue
	case ast.TableOptionEncryption:
		table.Options.Encryption = opt.StrValue
	case ast.TableOptionPackKeys:
		applyPackKeysOption(opt, table)
	case ast.TableOptionStatsPersistent:
		applyStatsPersistentOption(opt, table)
	case ast.TableOptionStatsAutoRecalc:
		applyStatsAutoRecalcOption(opt, table)
	case ast.TableOptionStatsSamplePages:
		applyStatsSamplePagesOption(opt, table)
	case ast.TableOptionStorageMedia:
		table.Options.StorageMedia = opt.StrValue
	case ast.TableOptionInsertMethod:
		table.Options.InsertMethod = opt.StrValue
	case ast.TableOptionConnection:
		table.Options.Connection = opt.StrValue
	case ast.TableOptionPassword:
		table.Options.Password = opt.StrValue
	case ast.TableOptionAutoextendSize:
		table.Options.AutoextendSize = opt.StrValue
	case ast.TableOptionPageChecksum:
		table.Options.PageChecksum = opt.UintValue
	case ast.TableOptionTransactional:
		table.Options.Transactional = opt.UintValue
	case ast.TableOptionNone:
	default:
		return false
	}
	return true
}

func applyPackKeysOption(opt *ast.TableOption, table *core.Table) {
	if opt.Default {
		table.Options.PackKeys = "DEFAULT"
	} else if opt.UintValue == 1 {
		table.Options.PackKeys = "1"
	} else {
		table.Options.PackKeys = "0"
	}
}

func applyStatsPersistentOption(opt *ast.TableOption, table *core.Table) {
	if opt.Default {
		table.Options.StatsPersistent = "DEFAULT"
	} else {
		table.Options.StatsPersistent = strconv.FormatUint(opt.UintValue, 10)
	}
}

func applyStatsAutoRecalcOption(opt *ast.TableOption, table *core.Table) {
	if opt.Default {
		table.Options.StatsAutoRecalc = "DEFAULT"
	} else {
		table.Options.StatsAutoRecalc = strconv.FormatUint(opt.UintValue, 10)
	}
}

func applyStatsSamplePagesOption(opt *ast.TableOption, table *core.Table) {
	if opt.Default {
		table.Options.StatsSamplePages = "DEFAULT"
	} else {
		table.Options.StatsSamplePages = strconv.FormatUint(opt.UintValue, 10)
	}
}

// TODO: refactor this logic to remove nolint comment
//
//nolint:revive // Large switch needed for MySQL-specific AST option mapping
func applyMySQLTableOption(opt *ast.TableOption, table *core.Table) bool {
	switch opt.Tp {
	case ast.TableOptionSecondaryEngine:
		table.Options.MySQL.SecondaryEngine = opt.StrValue
	case ast.TableOptionSecondaryEngineNull:
		table.Options.MySQL.SecondaryEngine = "NULL"
	case ast.TableOptionTableCheckSum:
		table.Options.MySQL.TableChecksum = opt.UintValue
	case ast.TableOptionUnion:
		applyUnionOption(opt, table)
	case ast.TableOptionEngineAttribute:
		table.Options.MySQL.EngineAttribute = opt.StrValue
	case ast.TableOptionSecondaryEngineAttribute:
		table.Options.MySQL.SecondaryEngineAttribute = opt.StrValue
	case ast.TableOptionPageCompressed:
		table.Options.MySQL.PageCompressed = optionTruthy(opt.BoolValue, opt.StrValue, opt.UintValue)
	case ast.TableOptionPageCompressionLevel:
		table.Options.MySQL.PageCompressionLevel = opt.UintValue
	case ast.TableOptionIetfQuotes:
		table.Options.MySQL.IetfQuotes = optionTruthy(opt.BoolValue, opt.StrValue, opt.UintValue)
	case ast.TableOptionNodegroup:
		table.Options.MySQL.Nodegroup = opt.UintValue
	default:
		return false
	}
	return true
}

func applyUnionOption(opt *ast.TableOption, table *core.Table) {
	table.Options.MySQL.Union = make([]string, len(opt.TableNames))
	for idx, tn := range opt.TableNames {
		table.Options.MySQL.Union[idx] = tn.Name.O
	}
}

// TODO: refactor this logic to remove nolint comment
//
//nolint:revive // Large switch needed for TiDB-specific AST option mapping
func (p *Parser) applyTiDBTableOption(opt *ast.TableOption, table *core.Table) bool {
	switch opt.Tp {
	case ast.TableOptionAutoIdCache:
		table.Options.TiDB.AutoIDCache = opt.UintValue
	case ast.TableOptionAutoRandomBase:
		table.Options.TiDB.AutoRandomBase = opt.UintValue
	case ast.TableOptionShardRowID:
		table.Options.TiDB.ShardRowID = opt.UintValue
	case ast.TableOptionPreSplitRegion:
		table.Options.TiDB.PreSplitRegion = opt.UintValue
	case ast.TableOptionTTL:
		p.applyTTLOption(opt, table)
	case ast.TableOptionTTLEnable:
		table.Options.TiDB.TTLEnable = optionTruthy(opt.BoolValue, opt.StrValue, opt.UintValue)
	case ast.TableOptionTTLJobInterval:
		table.Options.TiDB.TTLJobInterval = opt.StrValue
	case ast.TableOptionSequence:
		table.Options.TiDB.Sequence = optionTruthy(opt.BoolValue, opt.StrValue, opt.UintValue)
	case ast.TableOptionAffinity:
		table.Options.TiDB.Affinity = opt.StrValue
	case ast.TableOptionPlacementPolicy:
		table.Options.TiDB.PlacementPolicy = opt.StrValue
	case ast.TableOptionStatsBuckets:
		table.Options.TiDB.StatsBuckets = opt.UintValue
	case ast.TableOptionStatsTopN:
		table.Options.TiDB.StatsTopN = opt.UintValue
	case ast.TableOptionStatsColsChoice:
		table.Options.TiDB.StatsColsChoice = opt.StrValue
	case ast.TableOptionStatsColList:
		table.Options.TiDB.StatsColList = opt.StrValue
	case ast.TableOptionStatsSampleRate:
		p.parseTiDBStatsSampleRateOption(opt, table)
	default:
		return false
	}
	return true
}

func (p *Parser) applyTTLOption(opt *ast.TableOption, table *core.Table) {
	if opt.ColumnName != nil && opt.TimeUnitValue != nil {
		val := ""
		if opt.Value != nil {
			if s := p.exprToString(opt.Value); s != nil {
				val = *s
			}
		}
		table.Options.TiDB.TTL = fmt.Sprintf("`%s` + INTERVAL %s %s", opt.ColumnName.Name.O, val, opt.TimeUnitValue.Unit.String())
	}
}

func (p *Parser) parseTiDBStatsSampleRateOption(opt *ast.TableOption, table *core.Table) {
	if opt.Value == nil {
		return
	}
	s := p.exprToString(opt.Value)
	if s == nil {
		return
	}
	f, err := strconv.ParseFloat(*s, 64)
	if err != nil {
		return
	}
	table.Options.TiDB.StatsSampleRate = f
}

func optionTruthy(boolValue bool, strValue string, uintValue uint64) bool {
	if boolValue {
		return true
	}
	if uintValue == 1 {
		return true
	}
	s := strings.TrimSpace(strValue)
	return strings.EqualFold(s, "ON") || s == "1" || strings.EqualFold(s, "TRUE")
}

var rowFormatByValue = map[uint64]string{
	ast.RowFormatFixed:              "FIXED",
	ast.RowFormatDynamic:            "DYNAMIC",
	ast.RowFormatCompressed:         "COMPRESSED",
	ast.RowFormatRedundant:          "REDUNDANT",
	ast.RowFormatCompact:            "COMPACT",
	ast.RowFormatDefault:            "DEFAULT",
	ast.TokuDBRowFormatDefault:      "TOKUDB_DEFAULT",
	ast.TokuDBRowFormatFast:         "TOKUDB_FAST",
	ast.TokuDBRowFormatSmall:        "TOKUDB_SMALL",
	ast.TokuDBRowFormatZlib:         "TOKUDB_ZLIB",
	ast.TokuDBRowFormatQuickLZ:      "TOKUDB_QUICKLZ",
	ast.TokuDBRowFormatLzma:         "TOKUDB_LZMA",
	ast.TokuDBRowFormatSnappy:       "TOKUDB_SNAPPY",
	ast.TokuDBRowFormatUncompressed: "TOKUDB_UNCOMPRESSED",
	ast.TokuDBRowFormatZstd:         "TOKUDB_ZSTD",
}

func rowFormatToString(v uint64) string {
	if s, ok := rowFormatByValue[v]; ok {
		return s
	}
	return ""
}
