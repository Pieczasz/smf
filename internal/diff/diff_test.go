package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/parser"
)

var (
	diffTestOldSQL = `
CREATE TABLE std_options (
    id INT PRIMARY KEY
)
ENGINE = InnoDB
AUTO_INCREMENT = 10
AVG_ROW_LENGTH = 100
CHECKSUM = 1
COMPRESSION = 'ZLIB'
KEY_BLOCK_SIZE = 8
MAX_ROWS = 1000
MIN_ROWS = 100
DELAY_KEY_WRITE = 1
ROW_FORMAT = DYNAMIC
TABLESPACE = ts1
DATA DIRECTORY = '/tmp/data'
INDEX DIRECTORY = '/tmp/idx'
ENCRYPTION = 'Y'
STATS_AUTO_RECALC = DEFAULT
STATS_SAMPLE_PAGES = DEFAULT
INSERT_METHOD = FIRST;

CREATE TABLE std_options_numeric (
    id INT PRIMARY KEY
)
STATS_AUTO_RECALC = 1
STATS_SAMPLE_PAGES = 100;

CREATE TABLE add_options (
    id INT PRIMARY KEY
)
CONNECTION = 'mysql://user@host/db'
PASSWORD = 'secret_password'
AUTOEXTEND_SIZE = '64M'
PAGE_CHECKSUM = 1
TRANSACTIONAL = 1;

CREATE TABLE all_features (
    id INT AUTO_INCREMENT,
    t_tinyint TINYINT NOT NULL,
    t_varchar VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT 'default_val',
    t_text TEXT,
    t_enum ENUM('small', 'medium', 'large') DEFAULT 'medium',
    t_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    t_json JSON,
    g_col INT GENERATED ALWAYS AS (id + 1) VIRTUAL,
    g_col_stored INT GENERATED ALWAYS AS (id * 2) STORED,

    PRIMARY KEY (id),
    UNIQUE KEY idx_unique_varchar (t_varchar),
    KEY idx_regular (t_tinyint),
    CONSTRAINT chk_positive CHECK (id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Comprehensive table';

CREATE TABLE related_features (
    id INT PRIMARY KEY,
    f_id INT,
    CONSTRAINT fk_related FOREIGN KEY (f_id) REFERENCES all_features(id) ON DELETE CASCADE ON UPDATE RESTRICT
);`

	diffTestNewSQL = `
CREATE TABLE std_options (
    id INT PRIMARY KEY
)
ENGINE = MyISAM
AUTO_INCREMENT = 20
AVG_ROW_LENGTH = 200
CHECKSUM = 2
COMPRESSION = 'LZ4'
KEY_BLOCK_SIZE = 16
MAX_ROWS = 2000
MIN_ROWS = 50
DELAY_KEY_WRITE = 0
ROW_FORMAT = COMPACT
TABLESPACE = ts2
DATA DIRECTORY = '/tmp/data2'
INDEX DIRECTORY = '/tmp/idx2'
ENCRYPTION = 'N'
STATS_AUTO_RECALC = 1
STATS_SAMPLE_PAGES = 100
INSERT_METHOD = LAST;

CREATE TABLE std_options_numeric (
    id INT PRIMARY KEY
)
STATS_AUTO_RECALC = DEFAULT
STATS_SAMPLE_PAGES = 200;

CREATE TABLE add_options (
    id INT PRIMARY KEY
)
CONNECTION = 'mysql://user@host/db2'
PASSWORD = 'new_secret'
AUTOEXTEND_SIZE = '128M'
PAGE_CHECKSUM = 2
TRANSACTIONAL = 0;

CREATE TABLE all_features (
    id INT AUTO_INCREMENT,
    t_tinyint TINYINT NULL,
    t_varchar VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT 'changed',
    t_enum ENUM('small', 'medium', 'large') DEFAULT 'large',
    t_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    t_json JSON,
    new_col INT,
    g_col INT GENERATED ALWAYS AS (id + 2) VIRTUAL,
    g_col_stored INT GENERATED ALWAYS AS (id * 3) STORED,

    PRIMARY KEY (id),
	UNIQUE KEY idx_unique_varchar (t_enum),
	KEY idx_regular (t_enum),
    CONSTRAINT chk_positive CHECK (id >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Comprehensive table v2';

CREATE TABLE related_features (
    id INT PRIMARY KEY,
    f_id INT,
    CONSTRAINT fk_related FOREIGN KEY (f_id) REFERENCES all_features(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);`
)

func TestChangeSeverityStringFunctionality(t *testing.T) {
	assert.Equal(t, "INFO", SeverityInfo.String())
	assert.Equal(t, "WARNING", SeverityWarning.String())
	assert.Equal(t, "BREAKING", SeverityBreaking.String())
	assert.Equal(t, "CRITICAL", SeverityCritical.String())
	assert.Equal(t, "UNKNOWN", ChangeSeverity(99).String())
}

func TestSchemaDiffIsEmptyFunctionality(t *testing.T) {
	d := &SchemaDiff{}
	assert.True(t, d.IsEmpty())

	d.AddedTables = append(d.AddedTables, &core.Table{Name: "t"})
	assert.False(t, d.IsEmpty())

	d = &SchemaDiff{}
	d.RemovedTables = append(d.RemovedTables, &core.Table{Name: "t"})
	assert.False(t, d.IsEmpty())

	d = &SchemaDiff{}
	d.ModifiedTables = append(d.ModifiedTables, &TableDiff{Name: "t"})
	assert.False(t, d.IsEmpty())
}

func TestDiffGetNameFunctionality(t *testing.T) {
	td := &TableDiff{Name: "table"}
	assert.Equal(t, "table", td.GetName())

	cc := &ColumnChange{Name: "col"}
	assert.Equal(t, "col", cc.GetName())

	ctc := &ConstraintChange{Name: "const"}
	assert.Equal(t, "const", ctc.GetName())

	ic := &IndexChange{Name: "idx"}
	assert.Equal(t, "idx", ic.GetName())

	toc := &TableOptionChange{Name: "opt"}
	assert.Equal(t, "opt", toc.GetName())
}

func TestAnalyzeColumnChangesDetailsFunctionality(t *testing.T) {
	oldT := &core.Table{
		Name: "t",
		Columns: []*core.Column{
			{Name: "c1", Type: core.NormalizeDataType("INT"), AutoIncrement: true, PrimaryKey: true},
			{Name: "c2", Type: core.NormalizeDataType("VARCHAR(10)"), Charset: "utf8"},
			{Name: "c3", Type: core.NormalizeDataType("INT"), IsGenerated: true, GenerationExpression: "1", GenerationStorage: core.GenerationVirtual},
		},
	}
	newT := &core.Table{
		Name: "t",
		Columns: []*core.Column{
			{Name: "c1", Type: core.NormalizeDataType("INT"), AutoIncrement: false, PrimaryKey: false},
			{Name: "c2", Type: core.NormalizeDataType("VARCHAR(10)"), Charset: "latin1"},
			{Name: "c3", Type: core.NormalizeDataType("INT"), IsGenerated: false},
		},
	}

	d := Diff(&core.Database{Tables: []*core.Table{oldT}}, &core.Database{Tables: []*core.Table{newT}}, DefaultOptions())
	an := NewBreakingChangeAnalyzer()
	changes := an.Analyze(d)

	assert.True(t, hasBC(changes, SeverityWarning, "t", "c1", "AUTO_INCREMENT is being removed"))
	assert.True(t, hasBC(changes, SeverityBreaking, "t", "c1", "Primary key status changed"))
	assert.True(t, hasBC(changes, SeverityWarning, "t", "c2", "Character set changes"))
	assert.True(t, hasBC(changes, SeverityBreaking, "t", "c3", "Generated column status changed"))
}

func TestAnalyzeIndexAndConstraintRemovedFunctionality(t *testing.T) {
	oldT := &core.Table{
		Name:    "t",
		Columns: []*core.Column{{Name: "id", Type: core.NormalizeDataType("INT")}},
		Constraints: []*core.Constraint{
			{Name: "fk_1", Type: core.ConstraintForeignKey, Columns: []string{"id"}, ReferencedTable: "other", ReferencedColumns: []string{"id"}},
		},
		Indexes: []*core.Index{
			{Name: "idx_1", Columns: []core.IndexColumn{{Name: "id"}}},
		},
	}
	newT := &core.Table{
		Name:    "t",
		Columns: []*core.Column{{Name: "id", Type: core.NormalizeDataType("INT")}},
	}

	d := Diff(&core.Database{Tables: []*core.Table{oldT}}, &core.Database{Tables: []*core.Table{newT}}, DefaultOptions())
	an := NewBreakingChangeAnalyzer()
	changes := an.Analyze(d)

	assert.True(t, hasBC(changes, SeverityWarning, "t", "fk_1", "Foreign key will be dropped"))
	assert.True(t, hasBC(changes, SeverityInfo, "t", "idx_1", "Index will be dropped"))
}

func TestIndexComparisonDetailsFunctionality(t *testing.T) {
	idx1 := &core.Index{Name: "idx", Columns: []core.IndexColumn{{Name: "c1"}}, Unique: true, Type: core.IndexTypeBTree}
	idx2 := &core.Index{Name: "idx", Columns: []core.IndexColumn{{Name: "c1", Length: 10}}, Unique: true, Type: core.IndexTypeBTree}
	idx3 := &core.Index{Name: "idx", Columns: []core.IndexColumn{{Name: "c2"}}, Unique: true, Type: core.IndexTypeBTree}

	assert.False(t, equalIndex(idx1, idx2))
	assert.False(t, equalIndex(idx1, idx3))

	assert.NotEmpty(t, indexKey(idx1))
}

func TestConstraintComparisonDetailsFunctionality(t *testing.T) {
	c1 := &core.Constraint{Name: "fk", Type: core.ConstraintForeignKey, Columns: []string{"c1"}, ReferencedTable: "t2", ReferencedColumns: []string{"id"}, OnDelete: "RESTRICT"}
	c2 := &core.Constraint{Name: "fk", Type: core.ConstraintForeignKey, Columns: []string{"c1"}, ReferencedTable: "t2", ReferencedColumns: []string{"id"}, OnDelete: "CASCADE"}

	assert.False(t, equalConstraint(c1, c2))
	assert.NotEmpty(t, constraintKey(c1))

	col1 := &core.Column{Name: "c1", Type: "INT", TypeRaw: "INT"}
	col2 := &core.Column{Name: "c1", Type: "BIGINT", TypeRaw: "BIGINT"}

	t1 := &core.Table{
		Name:    "t",
		Columns: []*core.Column{col1},
		Constraints: []*core.Constraint{
			{Name: "fk", Type: core.ConstraintForeignKey, Columns: []string{"c1"}, ReferencedTable: "t2", ReferencedColumns: []string{"id"}},
		},
	}
	t2 := &core.Table{
		Name:    "t",
		Columns: []*core.Column{col2},
		Constraints: []*core.Constraint{
			{Name: "fk", Type: core.ConstraintForeignKey, Columns: []string{"c1"}, ReferencedTable: "t2", ReferencedColumns: []string{"id"}},
		},
	}

	d := compareTable(t1, t2, DefaultOptions())
	assert.NotNil(t, d)
	assert.NotEmpty(t, d.ModifiedConstraints)
	assert.True(t, d.ModifiedConstraints[0].RebuildOnly)
}

func TestHelpersDetailsFunctionality(t *testing.T) {
	assert.True(t, equalStringSliceCI([]string{"A", "B"}, []string{"a", "b"}))
	assert.False(t, equalStringSliceCI([]string{"A", "B"}, []string{"a", "c"}))
	assert.False(t, equalStringSliceCI([]string{"A", "B"}, []string{"a"}))

	tables := []*core.Table{{Name: "Table1"}, {Name: "table1"}}
	_, collisions := mapTablesByName(tables)
	assert.NotEmpty(t, collisions)

	cols := []*core.Column{{Name: "Col1"}, {Name: "col1"}}
	_, colCollisions := mapColumnsByName(cols)
	assert.NotEmpty(t, colCollisions)

	var items []Named
	items = append(items, &TableDiff{Name: "b"}, &TableDiff{Name: "a"})
	sortByFunc(items, func(n Named) string { return n.GetName() })
	assert.Equal(t, "a", items[0].GetName())
}

func TestIndexComparisonMoreDetailsFunctionality(t *testing.T) {
	idx1 := &core.Index{Columns: []core.IndexColumn{{Name: "c1", Length: 10, Order: "ASC"}}}
	idx2 := &core.Index{Columns: []core.IndexColumn{{Name: "c1", Length: 20, Order: "ASC"}}}
	idx3 := &core.Index{Columns: []core.IndexColumn{{Name: "c1", Length: 10, Order: "DESC"}}}
	idx4 := &core.Index{Columns: []core.IndexColumn{{Name: "c2"}}}

	assert.False(t, equalIndex(idx1, idx2))
	assert.False(t, equalIndex(idx1, idx3))
	assert.False(t, equalIndex(idx1, idx4))
}

func TestColumnRenameMoreDetailsFunctionality(t *testing.T) {
	oldC := &core.Column{Name: "old", Type: "INT", Comment: "shared"}
	newC := &core.Column{Name: "new", Type: "INT", Comment: "shared"}

	score := renameSimilarityScore(oldC, newC)
	assert.Greater(t, score, 0)

	assert.True(t, renameEvidenceWithTokens(oldC, newC, nil, nil))

	// Generated
	oldC2 := &core.Column{Name: "old", Type: "INT", IsGenerated: true, GenerationExpression: "1 + 1"}
	newC2 := &core.Column{Name: "new", Type: "INT", IsGenerated: true, GenerationExpression: "1 + 1"}
	assert.True(t, renameEvidenceWithTokens(oldC2, newC2, nil, nil))
}

func TestBreakingChangesEdgeCasesFunctionality(t *testing.T) {
	_, l, ok := parseTypeLength("VARCHAR(255)")
	assert.True(t, ok)
	assert.Equal(t, 255, l)

	_, _, ok = parseTypeLength("INT")
	assert.False(t, ok)

	an := NewBreakingChangeAnalyzer()
	severity := an.determineTypeMigrationSeverity("int", "bigint")
	assert.Equal(t, SeverityInfo, severity)

	severity = an.determineTypeMigrationSeverity("bigint", "int")
	assert.Equal(t, SeverityCritical, severity)

	severity = an.determineTypeMigrationSeverity("int", "varchar(10)")
	assert.Equal(t, SeverityCritical, severity)
}

func TestDiffWithCollisionsFunctionality(t *testing.T) {
	oldDB := &core.Database{Tables: []*core.Table{{Name: "T1"}, {Name: "t1"}}}
	newDB := &core.Database{Tables: []*core.Table{{Name: "T2"}, {Name: "t2"}}}

	d := Diff(oldDB, newDB, DefaultOptions())
	assert.NotEmpty(t, d.Warnings)
	assert.Contains(t, d.Warnings[0], "old schema:")
	assert.Contains(t, d.Warnings[1], "new schema:")
}

func TestIndexAndConstraintKeysFunctionality(t *testing.T) {
	idx := &core.Index{Unique: true, Type: core.IndexTypeBTree, Columns: []core.IndexColumn{{Name: "c1"}}}
	key := indexKey(idx)
	assert.Contains(t, key, "1:btree:c1")

	cons := &core.Constraint{Type: core.ConstraintUnique, Columns: []string{"c1"}}
	cKey := constraintKey(cons)
	assert.Contains(t, cKey, "unique:c1")
}

func TestConstraintFieldChangesFunctionality(t *testing.T) {
	c1 := &core.Constraint{Type: core.ConstraintForeignKey, ReferencedTable: "t1", OnDelete: "CASCADE"}
	c2 := &core.Constraint{Type: core.ConstraintForeignKey, ReferencedTable: "t2", OnDelete: "RESTRICT"}
	changes := constraintFieldChanges(c1, c2)
	assert.NotEmpty(t, changes)
}

func TestAnalyzeCharsetCollateChangeDetailsFunctionality(t *testing.T) {
	an := NewBreakingChangeAnalyzer()
	table := "t"
	ch := &ColumnChange{
		Name: "c",
		Old:  &core.Column{Charset: "utf8", Collate: "utf8_general_ci"},
		New:  &core.Column{Charset: "latin1", Collate: "latin1_swedish_ci"},
	}
	an.analyzeCharsetCollateChange(table, ch)
	assert.True(t, hasBC(an.Changes, SeverityWarning, table, "c", "Character set changes"))
	assert.True(t, hasBC(an.Changes, SeverityWarning, table, "c", "Collation changes"))
}

func TestTokenizeAndSharedTokensFunctionality(t *testing.T) {
	assert.Equal(t, []string{"user", "identifier"}, tokenizeName("user_identifier"))
	assert.Equal(t, []string{"user"}, tokenizeName("user_id")) // id is < 3

	assert.True(t, hasSharedTokens([]string{"user", "item"}, []string{"user", "name"}))
	assert.False(t, hasSharedTokens([]string{"user"}, []string{"item"}))
}

func TestAnalyzeConstraintTypesFunctionality(t *testing.T) {
	an := NewBreakingChangeAnalyzer()
	table := "t1"

	p1 := &core.Constraint{Name: "p1", Type: core.ConstraintPrimaryKey}
	fk1 := &core.Constraint{Name: "fk1", Type: core.ConstraintForeignKey}
	uq1 := &core.Constraint{Name: "uq1", Type: core.ConstraintUnique}
	ch1 := &core.Constraint{Name: "ch1", Type: core.ConstraintCheck}

	an.analyzeRemovedConstraints(table, []*core.Constraint{p1, fk1, uq1, ch1})
	assert.Equal(t, 4, len(an.Changes))

	an.Changes = nil
	an.analyzeAddedConstraints(table, []*core.Constraint{p1, fk1, uq1, ch1})
	assert.Equal(t, 4, len(an.Changes))

	an.Changes = nil
	modified := []*ConstraintChange{
		{Name: "m1", Old: p1, New: p1, RebuildOnly: false, RebuildReason: "test_reason"},
		{Name: "m2", Old: p1, New: p1, RebuildOnly: true, RebuildReason: "rebuild"},
	}
	an.analyzeModifiedConstraints(table, modified)
	assert.Equal(t, 1, len(an.Changes))
}

func TestEqualIndexVariationDetailsFunctionality(t *testing.T) {
	i1 := &core.Index{Name: "i", Comment: "c1", Visibility: "VISIBLE", Unique: true, Type: core.IndexTypeBTree, Columns: []core.IndexColumn{{Name: "c1"}}}
	i2 := &core.Index{Name: "i", Comment: "c1", Visibility: "VISIBLE", Unique: false, Type: core.IndexTypeBTree, Columns: []core.IndexColumn{{Name: "c1"}}}
	i3 := &core.Index{Name: "i", Comment: "c1", Visibility: "VISIBLE", Unique: true, Type: core.IndexTypeFullText, Columns: []core.IndexColumn{{Name: "c1"}}}
	i4 := &core.Index{Name: "i", Comment: "c2", Visibility: "VISIBLE", Unique: true, Type: core.IndexTypeBTree, Columns: []core.IndexColumn{{Name: "c1"}}}
	i5 := &core.Index{Name: "i", Comment: "c1", Visibility: "INVISIBLE", Unique: true, Type: core.IndexTypeBTree, Columns: []core.IndexColumn{{Name: "c1"}}}

	assert.False(t, equalIndex(i1, i2))
	assert.False(t, equalIndex(i1, i3))
	assert.False(t, equalIndex(i1, i4))
	assert.False(t, equalIndex(i1, i5))

	ic1 := []core.IndexColumn{{Name: "c1"}}
	ic2 := []core.IndexColumn{{Name: "c1"}, {Name: "c2"}}
	assert.False(t, equalIndexColumns(ic1, ic2))
}

func TestEqualConstraintDetailsFunctionality(t *testing.T) {
	c1 := &core.Constraint{Type: core.ConstraintForeignKey, Columns: []string{"a"}, ReferencedTable: "t2", ReferencedColumns: []string{"b"}, OnDelete: "CASCADE"}
	c2 := &core.Constraint{Type: core.ConstraintCheck, Columns: []string{"a"}}
	c3 := &core.Constraint{Type: core.ConstraintForeignKey, Columns: []string{"a"}, ReferencedTable: "t3", ReferencedColumns: []string{"b"}, OnDelete: "CASCADE"}
	c4 := &core.Constraint{Type: core.ConstraintForeignKey, Columns: []string{"a"}, ReferencedTable: "t2", ReferencedColumns: []string{"c"}, OnDelete: "CASCADE"}
	c5 := &core.Constraint{Type: core.ConstraintForeignKey, Columns: []string{"a"}, ReferencedTable: "t2", ReferencedColumns: []string{"b"}, OnDelete: "SET NULL"}
	c6 := &core.Constraint{Type: core.ConstraintForeignKey, Columns: []string{"a"}, ReferencedTable: "t2", ReferencedColumns: []string{"b"}, OnDelete: "CASCADE", OnUpdate: "RESTRICT"}
	c7 := &core.Constraint{Type: core.ConstraintCheck, CheckExpression: "a > 0"}
	c8 := &core.Constraint{Type: core.ConstraintCheck, CheckExpression: "a > 1"}

	assert.False(t, equalConstraint(c1, c2))
	assert.False(t, equalConstraint(c1, c3))
	assert.False(t, equalConstraint(c1, c4))
	assert.False(t, equalConstraint(c1, c5))
	assert.False(t, equalConstraint(c1, c6))
	assert.False(t, equalConstraint(c7, c8))
}

func TestRenameSimilarityScoreDetailsFunctionality(t *testing.T) {
	c1 := &core.Column{Name: "foo", TypeRaw: "int"}
	c2 := &core.Column{Name: "foo", TypeRaw: "varchar(10)"}
	c3 := &core.Column{Name: "bar", TypeRaw: "int"}

	assert.Equal(t, 0, renameSimilarityScore(c1, c2))   // Same name
	assert.Greater(t, renameSimilarityScore(c1, c3), 0) // Different name

	tokens1 := []string{"foo", "id"}
	tokens2 := []string{"foo", "new"}

	assert.True(t, renameEvidenceWithTokens(c1, c3, tokens1, tokens2)) // Shared "foo"

	c1.Comment = "test comment"
	c3.Comment = "test comment"
	assert.True(t, renameEvidenceWithTokens(c1, c3, nil, nil)) // Same comment

	c1.IsGenerated = true
	c3.IsGenerated = true
	c1.GenerationExpression = "a + b"
	c3.GenerationExpression = "a + b"
	assert.True(t, renameEvidenceWithTokens(c1, c3, nil, nil)) // Same generation expression
}

func TestAnalyzeModifiedIndexesFunctionality(t *testing.T) {
	an := NewBreakingChangeAnalyzer()
	table := "t1"

	modified := []*IndexChange{
		{Name: "idx1", Old: &core.Index{}, New: &core.Index{}},
		{Name: "idx2", Old: &core.Index{Unique: false}, New: &core.Index{Unique: true}},
	}
	an.analyzeModifiedIndexes(table, modified)
	assert.Equal(t, 2, len(an.Changes))
	assert.Equal(t, SeverityBreaking, an.Changes[1].Severity)
}

func TestAnalyzeColumnLengthChangeDetailsFunctionality(t *testing.T) {
	an := NewBreakingChangeAnalyzer()
	table := "t"

	ch := &ColumnChange{
		Name: "c",
		Old:  &core.Column{TypeRaw: "VARCHAR(10)"},
		New:  &core.Column{TypeRaw: "VARCHAR(20)"},
	}
	an.analyzeColumnLengthChange(table, ch)
	assert.True(t, hasBC(an.Changes, SeverityInfo, table, "c", "length increases"))

	ch2 := &ColumnChange{
		Name: "c2",
		Old:  &core.Column{TypeRaw: "VARCHAR(20)"},
		New:  &core.Column{TypeRaw: "VARCHAR(10)"},
	}
	an.analyzeColumnLengthChange(table, ch2)
	assert.True(t, hasBC(an.Changes, SeverityBreaking, table, "c2", "length shrinks"))

	ch3 := &ColumnChange{
		Name: "c3",
		Old:  &core.Column{TypeRaw: "VARCHAR(10)"},
		New:  &core.Column{TypeRaw: "CHAR(10)"},
	}
	lastLen := len(an.Changes)
	an.analyzeColumnLengthChange(table, ch3)
	assert.Equal(t, lastLen, len(an.Changes))
}

func TestIsValidRenameDetailsFunctionality(t *testing.T) {
	td := &TableDiff{}
	oldC := &core.Column{Name: "old", TypeRaw: "INT"}
	newC := &core.Column{Name: "new", TypeRaw: "INT"}

	assert.True(t, td.isValidRename(oldC, newC, []string{"user"}, []string{"user"}))

	newC2 := &core.Column{Name: "new", TypeRaw: "VARCHAR(10)"}
	assert.False(t, td.isValidRename(oldC, newC2, []string{"user"}, []string{"user"}))
}

func TestColumnRenameDetailsFunctionality(t *testing.T) {
	oldT := &core.Table{
		Name: "t",
		Columns: []*core.Column{
			{Name: "user_identifier", Type: "INT", TypeRaw: "INT"},
		},
	}
	newT := &core.Table{
		Name: "t",
		Columns: []*core.Column{
			{Name: "user_id", Type: "INT", TypeRaw: "INT"},
		},
	}

	d := Diff(&core.Database{Tables: []*core.Table{oldT}}, &core.Database{Tables: []*core.Table{newT}}, Options{DetectColumnRenames: true})
	assert.NotEmpty(t, d.ModifiedTables[0].RenamedColumns)
	assert.Equal(t, "user_identifier", d.ModifiedTables[0].RenamedColumns[0].Old.Name)
	assert.Equal(t, "user_id", d.ModifiedTables[0].RenamedColumns[0].New.Name)
}

func TestDiffFull(t *testing.T) {
	p := parser.NewSQLParser()
	oldDB, err := p.ParseSchema(diffTestOldSQL)
	require.NoError(t, err)
	newDB, err := p.ParseSchema(diffTestNewSQL)
	require.NoError(t, err)

	d := Diff(oldDB, newDB, DefaultOptions())
	require.NotNil(t, d)

	assert.Empty(t, d.AddedTables)
	assert.Empty(t, d.RemovedTables)
	require.GreaterOrEqual(t, len(d.ModifiedTables), 4)

	assertStdOptionsChanges(t, d)
	assertStdOptionsNumericChanges(t, d)
	assertAddOptionsChanges(t, d)
	assertAllFeaturesChanges(t, d)
	assertRelatedFeaturesChanges(t, d)
}

func findTableDiff(t *testing.T, d *SchemaDiff, name string) *TableDiff {
	t.Helper()
	for _, td := range d.ModifiedTables {
		if td != nil && td.Name == name {
			return td
		}
	}
	require.FailNow(t, "expected modified table diff not found", "table=%s", name)
	return nil
}

func assertOptionChange(t *testing.T, td *TableDiff, option string) {
	t.Helper()
	for _, ch := range td.ModifiedOptions {
		if ch != nil && ch.Name == option {
			assert.NotEqual(t, ch.Old, ch.New)
			return
		}
	}
	require.FailNow(t, "expected option change not found", "table=%s option=%s", td.Name, option)
}

func hasColumnChange(td *TableDiff, col string) bool {
	for _, ch := range td.ModifiedColumns {
		if ch != nil && ch.Name == col {
			return true
		}
	}
	return false
}

func hasAddedColumn(td *TableDiff, col string) bool {
	for _, c := range td.AddedColumns {
		if c != nil && c.Name == col {
			return true
		}
	}
	return false
}

func hasRemovedColumn(td *TableDiff, col string) bool {
	for _, c := range td.RemovedColumns {
		if c != nil && c.Name == col {
			return true
		}
	}
	return false
}

func hasModifiedConstraint(td *TableDiff, constraintName string) bool {
	for _, ch := range td.ModifiedConstraints {
		if ch != nil && ch.Name == constraintName {
			return true
		}
	}
	return false
}

func hasModifiedIndex(td *TableDiff, indexName string) bool {
	for _, ch := range td.ModifiedIndexes {
		if ch != nil && ch.Name == indexName {
			return true
		}
	}
	return false
}

func assertStdOptionsChanges(t *testing.T, d *SchemaDiff) {
	t.Helper()
	std := findTableDiff(t, d, "std_options")
	assertOptionChange(t, std, "ENGINE")
	assertOptionChange(t, std, "AUTO_INCREMENT")
	assertOptionChange(t, std, "AVG_ROW_LENGTH")
	assertOptionChange(t, std, "CHECKSUM")
	assertOptionChange(t, std, "COMPRESSION")
	assertOptionChange(t, std, "KEY_BLOCK_SIZE")
	assertOptionChange(t, std, "MAX_ROWS")
	assertOptionChange(t, std, "MIN_ROWS")
	assertOptionChange(t, std, "DELAY_KEY_WRITE")
	assertOptionChange(t, std, "ROW_FORMAT")
	assertOptionChange(t, std, "TABLESPACE")
	assertOptionChange(t, std, "DATA DIRECTORY")
	assertOptionChange(t, std, "INDEX DIRECTORY")
	assertOptionChange(t, std, "ENCRYPTION")
	assertOptionChange(t, std, "STATS_AUTO_RECALC")
	assertOptionChange(t, std, "STATS_SAMPLE_PAGES")
	assertOptionChange(t, std, "INSERT_METHOD")
}

func assertStdOptionsNumericChanges(t *testing.T, d *SchemaDiff) {
	t.Helper()
	stdNum := findTableDiff(t, d, "std_options_numeric")
	assertOptionChange(t, stdNum, "STATS_AUTO_RECALC")
	assertOptionChange(t, stdNum, "STATS_SAMPLE_PAGES")
}

func assertAddOptionsChanges(t *testing.T, d *SchemaDiff) {
	t.Helper()
	add := findTableDiff(t, d, "add_options")
	assertOptionChange(t, add, "CONNECTION")
	assertOptionChange(t, add, "PASSWORD")
	assertOptionChange(t, add, "AUTOEXTEND_SIZE")
	assertOptionChange(t, add, "PAGE_CHECKSUM")
	assertOptionChange(t, add, "TRANSACTIONAL")
}

func assertAllFeaturesChanges(t *testing.T, d *SchemaDiff) {
	t.Helper()
	af := findTableDiff(t, d, "all_features")
	assertOptionChange(t, af, "COMMENT")
	assert.True(t, hasColumnChange(af, "t_tinyint"))
	assert.True(t, hasColumnChange(af, "t_varchar"))
	assert.True(t, hasColumnChange(af, "t_enum"))
	assert.True(t, hasColumnChange(af, "t_timestamp"))
	assert.True(t, hasColumnChange(af, "g_col"))
	assert.True(t, hasColumnChange(af, "g_col_stored"))
	assert.True(t, hasAddedColumn(af, "new_col"))
	assert.True(t, hasRemovedColumn(af, "t_text"))
	assert.True(t, hasModifiedConstraint(af, "chk_positive"))
	assert.True(t, hasModifiedConstraint(af, "idx_unique_varchar"))
	assert.True(t, hasModifiedIndex(af, "idx_regular"))
}

func assertRelatedFeaturesChanges(t *testing.T, d *SchemaDiff) {
	t.Helper()
	rf := findTableDiff(t, d, "related_features")
	assert.True(t, hasModifiedConstraint(rf, "fk_related"))
}
