package tests

import (
	"smf/core"
	"smf/parser"
	"testing"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLParser(t *testing.T) {
	p := parser.NewSQLParser()

	sql := `
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
    FULLTEXT INDEX idx_fulltext (t_text),
    CONSTRAINT chk_positive CHECK (id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Comprehensive table';

CREATE TABLE multi_column_pk (
    a INT,
    b INT,
    PRIMARY KEY (a, b)
);

CREATE TABLE related_features (
    id INT PRIMARY KEY,
    f_id INT,
    CONSTRAINT fk_related FOREIGN KEY (f_id) REFERENCES all_features(id) ON DELETE CASCADE ON UPDATE RESTRICT
);
`

	db, err := p.ParseSchema(sql)
	require.NoError(t, err)
	require.Equal(t, 3, len(db.Tables))

	mcpk := db.FindTable("multi_column_pk")
	require.NotNil(t, mcpk)
	assert.True(t, mcpk.FindColumn("a").PrimaryKey)
	assert.True(t, mcpk.FindColumn("b").PrimaryKey)

	var pkConstraint *core.Constraint
	for _, c := range mcpk.Constraints {
		if c.Type == core.ConstraintPrimaryKey {
			pkConstraint = c
			break
		}
	}
	require.NotNil(t, pkConstraint)
	assert.Equal(t, []string{"a", "b"}, pkConstraint.Columns)

	af := db.FindTable("all_features")
	require.NotNil(t, af)
	assert.Equal(t, "Comprehensive table", af.Comment)
	assert.Equal(t, "utf8mb4", af.Options.Charset)
	assert.Equal(t, "utf8mb4_unicode_ci", af.Options.Collate)
	assert.Equal(t, "InnoDB", af.Options.Engine)

	idCol := af.FindColumn("id")
	require.NotNil(t, idCol)
	assert.True(t, idCol.AutoIncrement)
	assert.True(t, idCol.PrimaryKey)

	varcharCol := af.FindColumn("t_varchar")
	require.NotNil(t, varcharCol)
	assert.Equal(t, "utf8mb4_unicode_ci", varcharCol.Collate)
	assert.Equal(t, "utf8mb4", varcharCol.Charset)
	assert.Equal(t, "default_val", *varcharCol.DefaultValue)

	enumCol := af.FindColumn("t_enum")
	require.NotNil(t, enumCol)
	assert.Equal(t, "medium", *enumCol.DefaultValue)

	tsCol := af.FindColumn("t_timestamp")
	require.NotNil(t, tsCol)
	assert.NotNil(t, tsCol.DefaultValue)
	assert.NotNil(t, tsCol.OnUpdate)

	gCol := af.FindColumn("g_col")
	require.NotNil(t, gCol)
	assert.True(t, gCol.IsGenerated)
	assert.Equal(t, core.GenerationVirtual, gCol.GenerationStorage)
	assert.Contains(t, gCol.GenerationExpression, "id")

	gColStored := af.FindColumn("g_col_stored")
	require.NotNil(t, gColStored)
	assert.True(t, gColStored.IsGenerated)
	assert.Equal(t, core.GenerationStored, gColStored.GenerationStorage)

	assert.Len(t, af.Constraints, 3)

	var checkFound bool
	for _, c := range af.Constraints {
		if c.Type == core.ConstraintCheck {
			checkFound = true
			assert.Contains(t, c.CheckExpression, "id")
		}
	}
	assert.True(t, checkFound)

	assert.Len(t, af.Indexes, 2)

	rf := db.FindTable("related_features")
	require.NotNil(t, rf)
	var fkFound bool
	for _, c := range rf.Constraints {
		if c.Type == core.ConstraintForeignKey {
			fkFound = true
			assert.Equal(t, "all_features", c.ReferencedTable)
			assert.Equal(t, []string{"id"}, c.ReferencedColumns)
			assert.Equal(t, core.RefActionCascade, c.OnDelete)
			assert.Equal(t, core.RefActionRestrict, c.OnUpdate)
		}
	}
	assert.True(t, fkFound)
}
