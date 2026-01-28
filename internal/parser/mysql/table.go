package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"smf/internal/core"
)

func (p *Parser) parseTableOptions(opts []*ast.TableOption, table *core.Table) {
	for _, opt := range opts {
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
			if opt.Default {
				table.Options.PackKeys = "DEFAULT"
			} else if opt.UintValue == 1 {
				table.Options.PackKeys = "1"
			} else {
				table.Options.PackKeys = "0"
			}
		case ast.TableOptionStatsPersistent:
			if opt.Default {
				table.Options.StatsPersistent = "DEFAULT"
			} else {
				table.Options.StatsPersistent = strconv.FormatUint(opt.UintValue, 10)
			}
		case ast.TableOptionStatsAutoRecalc:
			if opt.Default {
				table.Options.StatsAutoRecalc = "DEFAULT"
			} else {
				table.Options.StatsAutoRecalc = strconv.FormatUint(opt.UintValue, 10)
			}
		case ast.TableOptionStatsSamplePages:
			if opt.Default {
				table.Options.StatsSamplePages = "DEFAULT"
			} else {
				table.Options.StatsSamplePages = strconv.FormatUint(opt.UintValue, 10)
			}
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

		case ast.TableOptionSecondaryEngine:
			table.Options.MySQL.SecondaryEngine = opt.StrValue
		case ast.TableOptionSecondaryEngineNull:
			table.Options.MySQL.SecondaryEngine = "NULL"
		case ast.TableOptionTableCheckSum:
			table.Options.MySQL.TableChecksum = opt.UintValue
		case ast.TableOptionUnion:
			table.Options.MySQL.Union = make([]string, len(opt.TableNames))
			for idx, tn := range opt.TableNames {
				table.Options.MySQL.Union[idx] = tn.Name.O
			}
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

		case ast.TableOptionAutoIdCache:
			table.Options.TiDB.AutoIDCache = opt.UintValue
		case ast.TableOptionAutoRandomBase:
			table.Options.TiDB.AutoRandomBase = opt.UintValue
		case ast.TableOptionShardRowID:
			table.Options.TiDB.ShardRowID = opt.UintValue
		case ast.TableOptionPreSplitRegion:
			table.Options.TiDB.PreSplitRegion = opt.UintValue
		case ast.TableOptionTTL:
			if opt.ColumnName != nil && opt.TimeUnitValue != nil {
				val := ""
				if opt.Value != nil {
					if s := p.exprToString(opt.Value); s != nil {
						val = *s
					}
				}
				table.Options.TiDB.TTL = fmt.Sprintf("`%s` + INTERVAL %s %s", opt.ColumnName.Name.O, val, opt.TimeUnitValue.Unit.String())
			}
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
			if opt.Value != nil {
				if s := p.exprToString(opt.Value); s != nil {
					if f, err := strconv.ParseFloat(*s, 64); err == nil {
						table.Options.TiDB.StatsSampleRate = f
					}
				}
			}
		case ast.TableOptionNone:
		}
	}
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

func rowFormatToString(v uint64) string {
	switch v {
	case ast.RowFormatFixed:
		return "FIXED"
	case ast.RowFormatDynamic:
		return "DYNAMIC"
	case ast.RowFormatCompressed:
		return "COMPRESSED"
	case ast.RowFormatRedundant:
		return "REDUNDANT"
	case ast.RowFormatCompact:
		return "COMPACT"
	case ast.RowFormatDefault:
		return "DEFAULT"
	case ast.TokuDBRowFormatDefault:
		return "TOKUDB_DEFAULT"
	case ast.TokuDBRowFormatFast:
		return "TOKUDB_FAST"
	case ast.TokuDBRowFormatSmall:
		return "TOKUDB_SMALL"
	case ast.TokuDBRowFormatZlib:
		return "TOKUDB_ZLIB"
	case ast.TokuDBRowFormatQuickLZ:
		return "TOKUDB_QUICKLZ"
	case ast.TokuDBRowFormatLzma:
		return "TOKUDB_LZMA"
	case ast.TokuDBRowFormatSnappy:
		return "TOKUDB_SNAPPY"
	case ast.TokuDBRowFormatUncompressed:
		return "TOKUDB_UNCOMPRESSED"
	case ast.TokuDBRowFormatZstd:
		return "TOKUDB_ZSTD"
	default:
		return ""
	}
}
