// Package mysql inside parser, provides implementation to parse MySQL schema dumps.
// It uses TiDB's parser, so we support both MySQL syntax and TiDB-specific options.
package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"smf/internal/core"
)

type Parser struct {
	p *parser.Parser
}

func NewParser() *Parser {
	return &Parser{p: parser.New()}
}

func (p *Parser) Parse(sql string) (*core.Database, error) {
	// TODO: add support to specify charset and collation
	// NOTE: this can be parallelized, it can help if schema dumps are big.
	stmtNodes, _, err := p.p.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	db := &core.Database{Tables: []*core.Table{}}
	for _, stmt := range stmtNodes {
		if create, ok := stmt.(*ast.CreateTableStmt); ok {
			table, err := p.convertCreateTable(create)
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
		Name:    stmt.Table.Name.O,
		Columns: []*core.Column{},
	}

	p.parseTableOptions(stmt.Options, table)
	p.parseColumns(stmt.Cols, table)
	p.parseConstraints(stmt.Constraints, table)

	return table, nil
}

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

func (p *Parser) parseColumns(cols []*ast.ColumnDef, table *core.Table) {
	for _, colDef := range cols {
		col := &core.Column{
			Name:     colDef.Name.Name.O,
			TypeRaw:  colDef.Tp.String(),
			Type:     core.NormalizeDataType(colDef.Tp.String()),
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
				col.Nullable = false
			case ast.ColumnOptionAutoIncrement:
				col.AutoIncrement = true
			case ast.ColumnOptionDefaultValue:
				col.DefaultValue = p.exprToString(opt.Expr)
			case ast.ColumnOptionOnUpdate:
				col.OnUpdate = p.exprToString(opt.Expr)
			case ast.ColumnOptionUniqKey:
				table.Constraints = append(table.Constraints, &core.Constraint{
					Type:    core.ConstraintUnique,
					Columns: []string{col.Name},
				})
			case ast.ColumnOptionComment:
				if s := p.exprToString(opt.Expr); s != nil {
					col.Comment = *s
				}
			case ast.ColumnOptionCollate:
				if s := p.exprToString(opt.Expr); s != nil {
					col.Collate = *s
				} else if opt.StrValue != "" {
					col.Collate = opt.StrValue
				}
			case ast.ColumnOptionFulltext:
				table.Indexes = append(table.Indexes, &core.Index{
					Columns: []core.IndexColumn{{Name: col.Name}},
					Unique:  false,
					Type:    core.IndexTypeFullText,
				})
			case ast.ColumnOptionCheck:
				if s := p.exprToString(opt.Expr); s != nil {
					table.Constraints = append(table.Constraints, &core.Constraint{
						Type:            core.ConstraintCheck,
						Columns:         []string{col.Name},
						CheckExpression: *s,
					})
				}
			case ast.ColumnOptionReference:
				c := &core.Constraint{
					Type:            core.ConstraintForeignKey,
					Columns:         []string{col.Name},
					ReferencedTable: opt.Refer.Table.Name.O,
				}
				for _, spec := range opt.Refer.IndexPartSpecifications {
					if spec.Column != nil {
						c.ReferencedColumns = append(c.ReferencedColumns, spec.Column.Name.O)
					}
				}
				if opt.Refer.OnDelete != nil {
					c.OnDelete = core.ReferentialAction(opt.Refer.OnDelete.ReferOpt.String())
				}
				if opt.Refer.OnUpdate != nil {
					c.OnUpdate = core.ReferentialAction(opt.Refer.OnUpdate.ReferOpt.String())
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
					col.GenerationStorage = core.GenerationStored
				} else {
					col.GenerationStorage = core.GenerationVirtual
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
		if col.PrimaryKey {
			p.ensurePrimaryKeyColumn(table, col.Name)
		}
	}
}

func (p *Parser) ensurePrimaryKeyColumn(table *core.Table, colName string) {
	if table == nil {
		return
	}
	colName = strings.TrimSpace(colName)
	if colName == "" {
		return
	}

	var pk *core.Constraint
	for _, c := range table.Constraints {
		if c == nil {
			continue
		}
		if c.Type == core.ConstraintPrimaryKey {
			pk = c
			break
		}
	}
	if pk == nil {
		pk = &core.Constraint{
			Name:    "PRIMARY",
			Type:    core.ConstraintPrimaryKey,
			Columns: []string{},
		}
		table.Constraints = append(table.Constraints, pk)
	}
	if strings.TrimSpace(pk.Name) == "" {
		pk.Name = "PRIMARY"
	}

	for _, existing := range pk.Columns {
		if strings.EqualFold(existing, colName) {
			if col := table.FindColumn(colName); col != nil {
				col.PrimaryKey = true
				col.Nullable = false
			}
			return
		}
	}
	pk.Columns = append(pk.Columns, colName)
	if col := table.FindColumn(colName); col != nil {
		col.PrimaryKey = true
		col.Nullable = false
	}
}

func (p *Parser) parseConstraints(constraints []*ast.Constraint, table *core.Table) {
	for _, constraint := range constraints {
		// TODO: check if make([]string, 0, len(constraint.Keys)) is faster or make([]string, len(constraint.Keys)) is faster
		columns := make([]string, 0, len(constraint.Keys))
		indexCols := make([]core.IndexColumn, 0, len(constraint.Keys))
		for _, key := range constraint.Keys {
			columns = append(columns, key.Column.Name.O)
			indexCols = append(indexCols, core.IndexColumn{
				Name:   key.Column.Name.O,
				Length: key.Length,
			})
		}

		switch constraint.Tp {
		case ast.ConstraintPrimaryKey:
			for _, colName := range columns {
				p.ensurePrimaryKeyColumn(table, colName)
			}
			if pk := table.PrimaryKey(); pk != nil {
				pk.Name = "PRIMARY"
				pk.Columns = columns
			}

		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			table.Constraints = append(table.Constraints, &core.Constraint{
				Name:    constraint.Name,
				Type:    core.ConstraintUnique,
				Columns: columns,
			})

		case ast.ConstraintForeignKey:
			c := &core.Constraint{
				Name:            constraint.Name,
				Type:            core.ConstraintForeignKey,
				Columns:         columns,
				ReferencedTable: constraint.Refer.Table.Name.O,
			}
			for _, spec := range constraint.Refer.IndexPartSpecifications {
				if spec.Column != nil {
					c.ReferencedColumns = append(c.ReferencedColumns, spec.Column.Name.O)
				}
			}
			if constraint.Refer.OnDelete != nil {
				c.OnDelete = core.ReferentialAction(constraint.Refer.OnDelete.ReferOpt.String())
			}
			if constraint.Refer.OnUpdate != nil {
				c.OnUpdate = core.ReferentialAction(constraint.Refer.OnUpdate.ReferOpt.String())
			}
			table.Constraints = append(table.Constraints, c)

		case ast.ConstraintIndex, ast.ConstraintKey:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: indexCols,
				Unique:  false,
				Type:    core.IndexTypeBTree,
			})

		case ast.ConstraintFulltext:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: indexCols,
				Unique:  false,
				Type:    core.IndexTypeFullText,
			})

		case ast.ConstraintCheck:
			c := &core.Constraint{
				Name:    constraint.Name,
				Type:    core.ConstraintCheck,
				Columns: columns,
			}
			if constraint.Expr != nil {
				if s := p.exprToString(constraint.Expr); s != nil {
					c.CheckExpression = *s
				}
			}
			table.Constraints = append(table.Constraints, c)

		case ast.ConstraintVector, ast.ConstraintColumnar:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: indexCols,
				Unique:  false,
				Type:    core.IndexTypeBTree,
			})
		case ast.ConstraintNoConstraint:
		}
	}
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
	s := strings.TrimSpace(sb.String())

	if unquoted, ok := tryUnquoteSQLStringLiteral(s); ok {
		return &unquoted
	}

	return &s
}

func tryUnquoteSQLStringLiteral(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[len(s)-1] != '\'' {
		return "", false
	}

	if s[0] == '\'' {
		return strings.ReplaceAll(s[1:len(s)-1], "''", "'"), true
	}

	q := strings.IndexByte(s, '\'')
	if q <= 0 {
		return "", false
	}
	prefix := strings.TrimSpace(s[:q])
	if !isSQLStringIntroducer(prefix) {
		return "", false
	}
	inner := s[q+1 : len(s)-1]
	return strings.ReplaceAll(inner, "''", "'"), true
}

func isSQLStringIntroducer(prefix string) bool {
	if prefix == "" {
		return false
	}
	if strings.EqualFold(prefix, "N") {
		return true
	}
	if !strings.HasPrefix(prefix, "_") || len(prefix) == 1 {
		return false
	}
	for _, r := range prefix[1:] {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_':
		default:
			return false
		}
	}
	return true
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
