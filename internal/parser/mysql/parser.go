package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"schemift/internal/core"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

type Parser struct {
	p *parser.Parser
}

func NewParser() *Parser {
	return &Parser{
		p: parser.New(),
	}
}

func (p *Parser) Parse(sql string) (*core.Database, error) {
	stmtNodes, _, err := p.p.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse MySQL dump: %v", err)
	}

	db := &core.Database{
		Tables: []*core.Table{},
	}

	for _, stmtNode := range stmtNodes {
		if createStmt, ok := stmtNode.(*ast.CreateTableStmt); ok {
			table, err := p.convertCreateTable(createStmt)
			if err != nil {
				return nil, err
			}
			db.Tables = append(db.Tables, table)
		}
	}

	return db, nil
}

func (p *Parser) convertCreateTable(stmt *ast.CreateTableStmt) (*core.Table, error) {
	table := &core.Table{
		Name:        stmt.Table.Name.O,
		Columns:     []*core.Column{},
		Constraints: []*core.Constraint{},
		Indexes:     []*core.Index{},
	}

	for _, opt := range stmt.Options {
		switch opt.Tp {
		case ast.TableOptionComment:
			table.Comment = opt.StrValue
		case ast.TableOptionCharset:
			table.Charset = opt.StrValue
		case ast.TableOptionCollate:
			table.Collate = opt.StrValue
		case ast.TableOptionEngine:
			table.Engine = opt.StrValue
		case ast.TableOptionAutoIncrement:
			table.AutoIncrement = opt.UintValue
		case ast.TableOptionAvgRowLength:
			table.AvgRowLength = opt.UintValue
		case ast.TableOptionCheckSum:
			table.Checksum = opt.UintValue
		case ast.TableOptionCompression:
			table.Compression = opt.StrValue
		case ast.TableOptionKeyBlockSize:
			table.KeyBlockSize = opt.UintValue
		case ast.TableOptionMaxRows:
			table.MaxRows = opt.UintValue
		case ast.TableOptionMinRows:
			table.MinRows = opt.UintValue
		case ast.TableOptionDelayKeyWrite:
			table.DelayKeyWrite = opt.UintValue
		case ast.TableOptionRowFormat:
			switch opt.UintValue {
			case ast.RowFormatFixed:
				table.RowFormat = "FIXED"
			case ast.RowFormatDynamic:
				table.RowFormat = "DYNAMIC"
			case ast.RowFormatCompressed:
				table.RowFormat = "COMPRESSED"
			case ast.RowFormatRedundant:
				table.RowFormat = "REDUNDANT"
			case ast.RowFormatCompact:
				table.RowFormat = "COMPACT"
			case ast.RowFormatDefault:
				table.RowFormat = "DEFAULT"
			case ast.TokuDBRowFormatDefault:
				table.RowFormat = "TOKUDB_DEFAULT"
			case ast.TokuDBRowFormatFast:
				table.RowFormat = "TOKUDB_FAST"
			case ast.TokuDBRowFormatSmall:
				table.RowFormat = "TOKUDB_SMALL"
			case ast.TokuDBRowFormatZlib:
				table.RowFormat = "TOKUDB_ZLIB"
			case ast.TokuDBRowFormatQuickLZ:
				table.RowFormat = "TOKUDB_QUICKLZ"
			case ast.TokuDBRowFormatLzma:
				table.RowFormat = "TOKUDB_LZMA"
			case ast.TokuDBRowFormatSnappy:
				table.RowFormat = "TOKUDB_SNAPPY"
			case ast.TokuDBRowFormatUncompressed:
				table.RowFormat = "TOKUDB_UNCOMPRESSED"
			case ast.TokuDBRowFormatZstd:
				table.RowFormat = "TOKUDB_ZSTD"
			}
		case ast.TableOptionTablespace:
			table.Tablespace = opt.StrValue
		case ast.TableOptionDataDirectory:
			table.DataDirectory = opt.StrValue
		case ast.TableOptionIndexDirectory:
			table.IndexDirectory = opt.StrValue
		case ast.TableOptionEncryption:
			table.Encryption = opt.StrValue
		case ast.TableOptionPackKeys:
			// Known TiDB parser limitation: The value for PACK_KEYS is parsed but not stored in the AST.
			// It always returns Default=false and UintValue=0 regardless of the input (0, 1, or DEFAULT).
			if opt.Default {
				table.PackKeys = "DEFAULT"
			} else if opt.UintValue == 1 {
				table.PackKeys = "1"
			} else {
				table.PackKeys = "0"
			}
		case ast.TableOptionStatsPersistent:
			// Known TiDB parser limitation: The value for STATS_PERSISTENT is parsed but not stored in the AST.
			// It always returns Default=false and UintValue=0 regardless of the input (0, 1, or DEFAULT).
			if opt.Default {
				table.StatsPersistent = "DEFAULT"
			} else {
				table.StatsPersistent = strconv.FormatUint(opt.UintValue, 10)
			}
		case ast.TableOptionStatsAutoRecalc:
			if opt.Default {
				table.StatsAutoRecalc = "DEFAULT"
			} else {
				table.StatsAutoRecalc = strconv.FormatUint(opt.UintValue, 10)
			}
		case ast.TableOptionStatsSamplePages:
			if opt.Default {
				table.StatsSamplePages = "DEFAULT"
			} else {
				table.StatsSamplePages = strconv.FormatUint(opt.UintValue, 10)
			}
		case ast.TableOptionStorageMedia:
			table.StorageMedia = opt.StrValue
		case ast.TableOptionInsertMethod:
			table.InsertMethod = opt.StrValue
		case ast.TableOptionNone:
		case ast.TableOptionAutoIdCache:
			table.AutoIdCache = opt.UintValue
		case ast.TableOptionAutoRandomBase:
			table.AutoRandomBase = opt.UintValue
		case ast.TableOptionConnection:
			table.Connection = opt.StrValue
		case ast.TableOptionPassword:
			table.Password = opt.StrValue
		case ast.TableOptionShardRowID:
			table.ShardRowID = opt.UintValue
		case ast.TableOptionPreSplitRegion:
			table.PreSplitRegion = opt.UintValue
		case ast.TableOptionNodegroup:
			table.Nodegroup = opt.UintValue
		case ast.TableOptionSecondaryEngine:
			table.SecondaryEngine = opt.StrValue
		case ast.TableOptionSecondaryEngineNull:
			table.SecondaryEngine = "NULL"
		case ast.TableOptionTableCheckSum:
			table.TableChecksum = opt.UintValue
		case ast.TableOptionUnion:
			table.Union = make([]string, 0, len(opt.TableNames))
			for _, tn := range opt.TableNames {
				table.Union = append(table.Union, tn.Name.O)
			}
		case ast.TableOptionTTL:
			if opt.ColumnName != nil && opt.TimeUnitValue != nil {
				val := ""
				if opt.Value != nil {
					if s := p.exprToString(opt.Value); s != nil {
						val = *s
					}
				}
				table.TTL = fmt.Sprintf("`%s` + INTERVAL %s %s", opt.ColumnName.Name.O, val, opt.TimeUnitValue.Unit.String())
			}
		case ast.TableOptionTTLEnable:
			table.TTLEnable = opt.BoolValue
			if !table.TTLEnable && (strings.EqualFold(opt.StrValue, "ON") || strings.EqualFold(opt.StrValue, "1")) {
				table.TTLEnable = true
			}
		case ast.TableOptionTTLJobInterval:
			table.TTLJobInterval = opt.StrValue
		case ast.TableOptionEngineAttribute:
			table.EngineAttribute = opt.StrValue
		case ast.TableOptionSecondaryEngineAttribute:
			table.SecondaryEngineAttribute = opt.StrValue
		case ast.TableOptionAutoextendSize:
			table.AutoextendSize = opt.StrValue
		case ast.TableOptionPageChecksum:
			table.PageChecksum = opt.UintValue
		case ast.TableOptionPageCompressed:
			table.PageCompressed = opt.BoolValue
		case ast.TableOptionPageCompressionLevel:
			table.PageCompressionLevel = opt.UintValue
		case ast.TableOptionTransactional:
			table.Transactional = opt.UintValue
		case ast.TableOptionIetfQuotes:
			table.IetfQuotes = opt.BoolValue
			if !table.IetfQuotes && (strings.EqualFold(opt.StrValue, "ON") || strings.EqualFold(opt.StrValue, "1")) {
				table.IetfQuotes = true
			}
		case ast.TableOptionSequence:
			table.Sequence = opt.BoolValue
		case ast.TableOptionAffinity:
			table.Affinity = opt.StrValue
		case ast.TableOptionPlacementPolicy:
			table.PlacementPolicy = opt.StrValue
		case ast.TableOptionStatsBuckets:
			table.StatsBuckets = opt.UintValue
		case ast.TableOptionStatsTopN:
			table.StatsTopN = opt.UintValue
		case ast.TableOptionStatsColsChoice:
			table.StatsColsChoice = opt.StrValue
		case ast.TableOptionStatsColList:
			table.StatsColList = opt.StrValue
		case ast.TableOptionStatsSampleRate:
			if opt.Value != nil {
				if s := p.exprToString(opt.Value); s != nil {
					if f, err := strconv.ParseFloat(*s, 64); err == nil {
						table.StatsSampleRate = f
					}
				}
			}
		}
	}

	for _, colDef := range stmt.Cols {
		col := &core.Column{
			Name:     colDef.Name.Name.O,
			TypeRaw:  colDef.Tp.String(),
			Type:     normalizeType(colDef.Tp.String()),
			Nullable: true,
			Collate:  colDef.Tp.GetCollate(),
			Charset:  colDef.Tp.GetCharset(),
		}

		for _, opt := range colDef.Options {
			switch opt.Tp {
			case ast.ColumnOptionNotNull:
				col.Nullable = false
			case ast.ColumnOptionNull:
				col.Nullable = true
			case ast.ColumnOptionPrimaryKey:
				col.PrimaryKey = true
			case ast.ColumnOptionAutoIncrement:
				col.AutoIncrement = true
			case ast.ColumnOptionDefaultValue:
				col.DefaultValue = p.exprToString(opt.Expr)
			case ast.ColumnOptionOnUpdate:
				col.OnUpdate = p.exprToString(opt.Expr)
			case ast.ColumnOptionUniqKey:
				table.Constraints = append(table.Constraints, &core.Constraint{
					Type:    core.Unique,
					Columns: []string{col.Name},
				})
			case ast.ColumnOptionComment:
				if s := p.exprToString(opt.Expr); s != nil {
					col.Comment = *s
				}
			case ast.ColumnOptionCollate:
				if s := p.exprToString(opt.Expr); s != nil {
					col.Collate = *s
				} else {
					col.Collate = opt.StrValue
				}
			case ast.ColumnOptionFulltext:
				table.Indexes = append(table.Indexes, &core.Index{
					Columns: []string{col.Name},
					Unique:  false,
					Type:    "FULLTEXT",
				})
			case ast.ColumnOptionCheck:
				if s := p.exprToString(opt.Expr); s != nil {
					table.Constraints = append(table.Constraints, &core.Constraint{
						Type:            core.Check,
						Columns:         []string{col.Name},
						CheckExpression: *s,
					})
				}
			case ast.ColumnOptionReference:
				c := &core.Constraint{
					Type:            core.ForeignKey,
					Columns:         []string{col.Name},
					ReferencedTable: opt.Refer.Table.Name.O,
				}
				refCols := make([]string, 0, len(opt.Refer.IndexPartSpecifications))
				for _, spec := range opt.Refer.IndexPartSpecifications {
					if spec.Column != nil {
						refCols = append(refCols, spec.Column.Name.O)
					}
				}
				c.ReferencedColumns = refCols
				if opt.Refer.OnDelete != nil {
					c.OnDelete = opt.Refer.OnDelete.ReferOpt.String()
				}
				if opt.Refer.OnUpdate != nil {
					c.OnUpdate = opt.Refer.OnUpdate.ReferOpt.String()
				}
				table.Constraints = append(table.Constraints, c)
			case ast.ColumnOptionGenerated:
				col.IsGenerated = true
				if opt.Expr != nil {
					if s := p.exprToString(opt.Expr); s != nil {
						col.GenerationExpression = *s
					}
				}
				if opt.Stored {
					col.GenerationStorage = "STORED"
				} else {
					col.GenerationStorage = "VIRTUAL"
				}
			case ast.ColumnOptionColumnFormat:
				col.ColumnFormat = opt.StrValue
			case ast.ColumnOptionStorage:
				col.Storage = opt.StrValue
			case ast.ColumnOptionAutoRandom:
				col.AutoRandom = uint64(opt.AutoRandOpt.ShardBits)
			case ast.ColumnOptionSecondaryEngineAttribute:
				col.SecondaryEngineAttribute = opt.StrValue
			case ast.ColumnOptionNoOption:
			}
		}
		table.Columns = append(table.Columns, col)
	}

	for _, constraint := range stmt.Constraints {
		c := &core.Constraint{
			Name: constraint.Name,
		}

		columns := make([]string, 0, len(constraint.Keys))
		for _, key := range constraint.Keys {
			columns = append(columns, key.Column.Name.O)
		}
		c.Columns = columns

		switch constraint.Tp {
		case ast.ConstraintPrimaryKey:
			c.Type = core.PrimaryKey
			c.Name = "PRIMARY"

			for _, colName := range columns {
				if col := table.FindColumn(colName); col != nil {
					col.PrimaryKey = true
				}
			}
			table.Constraints = append(table.Constraints, c)

		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			c.Type = core.Unique
			table.Constraints = append(table.Constraints, c)

		case ast.ConstraintForeignKey:
			c.Type = core.ForeignKey
			c.ReferencedTable = constraint.Refer.Table.Name.O
			refCols := make([]string, 0, len(constraint.Refer.IndexPartSpecifications))
			for _, spec := range constraint.Refer.IndexPartSpecifications {
				if spec.Column != nil {
					refCols = append(refCols, spec.Column.Name.O)
				}
			}
			c.ReferencedColumns = refCols
			if constraint.Refer.OnDelete != nil {
				c.OnDelete = constraint.Refer.OnDelete.ReferOpt.String()
			}
			if constraint.Refer.OnUpdate != nil {
				c.OnUpdate = constraint.Refer.OnUpdate.ReferOpt.String()
			}
			table.Constraints = append(table.Constraints, c)

		case ast.ConstraintIndex, ast.ConstraintKey:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: columns,
				Unique:  false,
				Type:    "BTREE",
			})
		case ast.ConstraintFulltext:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: columns,
				Unique:  false,
				Type:    "FULLTEXT",
			})
		case ast.ConstraintCheck:
			c.Type = core.Check
			if constraint.Expr != nil {
				if s := p.exprToString(constraint.Expr); s != nil {
					c.CheckExpression = *s
				}
			}
			table.Constraints = append(table.Constraints, c)
		case ast.ConstraintVector, ast.ConstraintColumnar:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: columns,
				Unique:  false,
				Type:    "INDEX",
			})
		case ast.ConstraintNoConstraint:

		}
	}

	return table, nil
}

func (p *Parser) exprToString(expr ast.ExprNode) *string {
	if expr == nil {
		return nil
	}
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := expr.Restore(restoreCtx); err != nil {
		return nil
	}
	s := sb.String()

	if strings.Contains(s, "'") {
		start := strings.Index(s, "'")
		end := strings.LastIndex(s, "'")
		if start != -1 && end != -1 && start < end {
			s = s[start+1 : end]
		}
	}

	return &s
}

func normalizeType(rawType string) string {
	rawType = strings.ToLower(strings.TrimSpace(rawType))

	if strings.Contains(rawType, "char") || strings.Contains(rawType, "text") || strings.Contains(rawType, "string") || strings.Contains(rawType, "enum") || strings.Contains(rawType, "set") {
		return "string"
	}

	if strings.Contains(rawType, "int") {
		return "int"
	}

	if strings.Contains(rawType, "float") || strings.Contains(rawType, "double") || strings.Contains(rawType, "decimal") || strings.Contains(rawType, "numeric") {
		return "float"
	}

	if strings.Contains(rawType, "bool") || rawType == "tinyint(1)" {
		return "boolean"
	}

	if strings.Contains(rawType, "date") || strings.Contains(rawType, "time") || strings.Contains(rawType, "timestamp") {
		return "datetime"
	}

	if strings.Contains(rawType, "json") {
		return "json"
	}

	if strings.Contains(rawType, "uuid") {
		return "uuid"
	}

	return rawType
}
