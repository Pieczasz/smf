package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"smf/internal/core"
	"smf/internal/diff"
)

func TestIndexDefinitionInlineRegular(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "idx_email",
		Columns: []core.IndexColumn{{Name: "email"}},
	}

	result := g.indexDefinitionInline(idx)

	assert.Equal(t, "KEY `idx_email` (`email`)", result)
}

func TestIndexDefinitionInlineUnique(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "uq_email",
		Columns: []core.IndexColumn{{Name: "email"}},
		Unique:  true,
	}

	result := g.indexDefinitionInline(idx)

	assert.Equal(t, "UNIQUE KEY `uq_email` (`email`)", result)
}

func TestIndexDefinitionInlineFulltext(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "ft_content",
		Columns: []core.IndexColumn{{Name: "content"}},
		Type:    "FULLTEXT",
	}

	result := g.indexDefinitionInline(idx)

	assert.Equal(t, "FULLTEXT KEY `ft_content` (`content`)", result)
}

func TestIndexDefinitionInlineSpatial(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "sp_location",
		Columns: []core.IndexColumn{{Name: "location"}},
		Type:    "SPATIAL",
	}

	result := g.indexDefinitionInline(idx)

	assert.Equal(t, "SPATIAL KEY `sp_location` (`location`)", result)
}

func TestIndexDefinitionInlineEmptyName(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "",
		Columns: []core.IndexColumn{{Name: "email"}},
	}

	result := g.indexDefinitionInline(idx)

	assert.Equal(t, "", result)
}

func TestIndexDefinitionInlineWhitespaceName(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "   ",
		Columns: []core.IndexColumn{{Name: "email"}},
	}

	result := g.indexDefinitionInline(idx)

	assert.Equal(t, "", result)
}

func TestConstraintDefinitionPrimaryKey(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:    core.ConstraintPrimaryKey,
		Columns: []string{"id"},
	}

	result := g.constraintDefinition(c)

	assert.Equal(t, "PRIMARY KEY (`id`)", result)
}

func TestConstraintDefinitionPrimaryKeyComposite(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:    core.ConstraintPrimaryKey,
		Columns: []string{"id", "tenant_id"},
	}

	result := g.constraintDefinition(c)

	assert.Equal(t, "PRIMARY KEY (`id`, `tenant_id`)", result)
}

func TestConstraintDefinitionUniqueWithName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:    "uq_email",
		Type:    core.ConstraintUnique,
		Columns: []string{"email"},
	}

	result := g.constraintDefinition(c)

	assert.Equal(t, "CONSTRAINT `uq_email` UNIQUE KEY (`email`)", result)
}

func TestConstraintDefinitionUniqueWithoutName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:    core.ConstraintUnique,
		Columns: []string{"email"},
	}

	result := g.constraintDefinition(c)

	assert.Equal(t, "UNIQUE KEY (`email`)", result)
}

func TestConstraintDefinitionCheckWithName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:            "chk_age",
		Type:            core.ConstraintCheck,
		CheckExpression: "age >= 0",
	}

	result := g.constraintDefinition(c)

	assert.Equal(t, "CONSTRAINT `chk_age` CHECK (age >= 0)", result)
}

func TestConstraintDefinitionCheckWithoutName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:            core.ConstraintCheck,
		CheckExpression: "age >= 0",
	}

	result := g.constraintDefinition(c)

	assert.Equal(t, "CHECK (age >= 0)", result)
}

func TestConstraintDefinitionCheckEmptyExpression(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name: "chk_empty",
		Type: core.ConstraintCheck,
	}

	result := g.constraintDefinition(c)

	assert.Equal(t, "", result)
}

func TestConstraintDefinitionForeignKeyReturnsEmpty(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:              "fk_user",
		Type:              core.ConstraintForeignKey,
		Columns:           []string{"user_id"},
		ReferencedTable:   "users",
		ReferencedColumns: []string{"id"},
	}

	result := g.constraintDefinition(c)

	assert.Equal(t, "", result)
}

func TestConstraintDefinitionUnknownType(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type: core.ConstraintType("unknown"),
	}

	result := g.constraintDefinition(c)

	assert.Equal(t, "", result)
}

func TestDropConstraintPrimaryKey(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:    core.ConstraintPrimaryKey,
		Columns: []string{"id"},
	}

	result := g.dropConstraint("`users`", c)

	assert.Equal(t, "ALTER TABLE `users` DROP PRIMARY KEY;", result)
}

func TestDropConstraintForeignKeyWithName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:    "fk_user",
		Type:    core.ConstraintForeignKey,
		Columns: []string{"user_id"},
	}

	result := g.dropConstraint("`orders`", c)

	assert.Equal(t, "ALTER TABLE `orders` DROP FOREIGN KEY `fk_user`;", result)
}

func TestDropConstraintForeignKeyWithoutName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:    core.ConstraintForeignKey,
		Columns: []string{"user_id"},
	}

	result := g.dropConstraint("`orders`", c)

	assert.Contains(t, result, "-- cannot drop unnamed FOREIGN KEY")
	assert.Contains(t, result, "user_id")
}

func TestDropConstraintForeignKeyWithoutNameNoColumns(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:    core.ConstraintForeignKey,
		Columns: []string{},
	}

	result := g.dropConstraint("`orders`", c)

	assert.Contains(t, result, "-- cannot drop unnamed FOREIGN KEY")
}

func TestDropConstraintUniqueWithName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:    "uq_email",
		Type:    core.ConstraintUnique,
		Columns: []string{"email"},
	}

	result := g.dropConstraint("`users`", c)

	assert.Equal(t, "ALTER TABLE `users` DROP INDEX `uq_email`;", result)
}

func TestDropConstraintUniqueWithoutName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:    core.ConstraintUnique,
		Columns: []string{"email"},
	}

	result := g.dropConstraint("`users`", c)

	assert.Contains(t, result, "-- cannot drop unnamed UNIQUE")
	assert.Contains(t, result, "email")
}

func TestDropConstraintUniqueWithoutNameNoColumns(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:    core.ConstraintUnique,
		Columns: []string{},
	}

	result := g.dropConstraint("`users`", c)

	assert.Contains(t, result, "-- cannot drop unnamed UNIQUE")
}

func TestDropConstraintCheckWithName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:            "chk_age",
		Type:            core.ConstraintCheck,
		CheckExpression: "age >= 0",
	}

	result := g.dropConstraint("`users`", c)

	assert.Equal(t, "ALTER TABLE `users` DROP CHECK `chk_age`;", result)
}

func TestDropConstraintCheckWithoutName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:            core.ConstraintCheck,
		CheckExpression: "age >= 0",
	}

	result := g.dropConstraint("`users`", c)

	assert.Contains(t, result, "-- cannot drop unnamed CHECK")
}

func TestDropConstraintNil(t *testing.T) {
	g := NewMySQLGenerator()

	result := g.dropConstraint("`users`", nil)

	assert.Equal(t, "", result)
}

func TestDropConstraintUnknownType(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type: core.ConstraintType("unknown"),
	}

	result := g.dropConstraint("`users`", c)

	assert.Equal(t, "", result)
}

func TestAddConstraintPrimaryKey(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:    core.ConstraintPrimaryKey,
		Columns: []string{"id"},
	}

	result := g.addConstraint("`users`", c)

	assert.Equal(t, "ALTER TABLE `users` ADD PRIMARY KEY (`id`);", result)
}

func TestAddConstraintUniqueWithName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:    "uq_email",
		Type:    core.ConstraintUnique,
		Columns: []string{"email"},
	}

	result := g.addConstraint("`users`", c)

	assert.Equal(t, "ALTER TABLE `users` ADD CONSTRAINT `uq_email` UNIQUE (`email`);", result)
}

func TestAddConstraintUniqueWithoutName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:    core.ConstraintUnique,
		Columns: []string{"email"},
	}

	result := g.addConstraint("`users`", c)

	assert.Equal(t, "ALTER TABLE `users` ADD UNIQUE (`email`);", result)
}

func TestAddConstraintForeignKey(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:              "fk_user",
		Type:              core.ConstraintForeignKey,
		Columns:           []string{"user_id"},
		ReferencedTable:   "users",
		ReferencedColumns: []string{"id"},
		OnDelete:          "CASCADE",
		OnUpdate:          "NO ACTION",
	}

	result := g.addConstraint("`orders`", c)

	assert.Contains(t, result, "ALTER TABLE `orders` ADD")
	assert.Contains(t, result, "CONSTRAINT `fk_user`")
	assert.Contains(t, result, "FOREIGN KEY (`user_id`)")
	assert.Contains(t, result, "REFERENCES `users` (`id`)")
	assert.Contains(t, result, "ON DELETE CASCADE")
	assert.Contains(t, result, "ON UPDATE NO ACTION")
}

func TestAddConstraintForeignKeyWithoutActions(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:              "fk_user",
		Type:              core.ConstraintForeignKey,
		Columns:           []string{"user_id"},
		ReferencedTable:   "users",
		ReferencedColumns: []string{"id"},
	}

	result := g.addConstraint("`orders`", c)

	assert.Contains(t, result, "FOREIGN KEY")
	assert.NotContains(t, result, "ON DELETE")
	assert.NotContains(t, result, "ON UPDATE")
}

func TestAddConstraintForeignKeyEmptyColumns(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:              "fk_user",
		Type:              core.ConstraintForeignKey,
		Columns:           []string{},
		ReferencedTable:   "users",
		ReferencedColumns: []string{"id"},
	}

	result := g.addConstraint("`orders`", c)

	assert.Equal(t, "", result)
}

func TestAddConstraintForeignKeyEmptyReferencedTable(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:              "fk_user",
		Type:              core.ConstraintForeignKey,
		Columns:           []string{"user_id"},
		ReferencedTable:   "",
		ReferencedColumns: []string{"id"},
	}

	result := g.addConstraint("`orders`", c)

	assert.Equal(t, "", result)
}

func TestAddConstraintCheckWithName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name:            "chk_age",
		Type:            core.ConstraintCheck,
		CheckExpression: "age >= 0",
	}

	result := g.addConstraint("`users`", c)

	assert.Equal(t, "ALTER TABLE `users` ADD CONSTRAINT `chk_age` CHECK (age >= 0);", result)
}

func TestAddConstraintCheckWithoutName(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type:            core.ConstraintCheck,
		CheckExpression: "age >= 0",
	}

	result := g.addConstraint("`users`", c)

	assert.Equal(t, "ALTER TABLE `users` ADD CHECK (age >= 0);", result)
}

func TestAddConstraintCheckEmptyExpression(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Name: "chk_empty",
		Type: core.ConstraintCheck,
	}

	result := g.addConstraint("`users`", c)

	assert.Equal(t, "", result)
}

func TestAddConstraintNil(t *testing.T) {
	g := NewMySQLGenerator()

	result := g.addConstraint("`users`", nil)

	assert.Equal(t, "", result)
}

func TestAddConstraintUnknownType(t *testing.T) {
	g := NewMySQLGenerator()

	c := &core.Constraint{
		Type: core.ConstraintType("unknown"),
	}

	result := g.addConstraint("`users`", c)

	assert.Equal(t, "", result)
}

func TestCreateIndexRegular(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "idx_email",
		Columns: []core.IndexColumn{{Name: "email"}},
	}

	result := g.createIndex("`users`", idx)

	assert.Equal(t, "CREATE INDEX `idx_email` ON `users` (`email`);", result)
}

func TestCreateIndexUnique(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "uq_email",
		Columns: []core.IndexColumn{{Name: "email"}},
		Unique:  true,
	}

	result := g.createIndex("`users`", idx)

	assert.Equal(t, "CREATE UNIQUE INDEX `uq_email` ON `users` (`email`);", result)
}

func TestCreateIndexFulltext(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "ft_content",
		Columns: []core.IndexColumn{{Name: "content"}},
		Type:    "FULLTEXT",
	}

	result := g.createIndex("`posts`", idx)

	assert.Equal(t, "CREATE FULLTEXT INDEX `ft_content` ON `posts` (`content`);", result)
}

func TestCreateIndexSpatial(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "sp_location",
		Columns: []core.IndexColumn{{Name: "location"}},
		Type:    "SPATIAL",
	}

	result := g.createIndex("`places`", idx)

	assert.Equal(t, "CREATE SPATIAL INDEX `sp_location` ON `places` (`location`);", result)
}

func TestCreateIndexNil(t *testing.T) {
	g := NewMySQLGenerator()

	result := g.createIndex("`users`", nil)

	assert.Equal(t, "", result)
}

func TestCreateIndexEmptyName(t *testing.T) {
	g := NewMySQLGenerator()

	idx := &core.Index{
		Name:    "",
		Columns: []core.IndexColumn{{Name: "email"}},
	}

	result := g.createIndex("`users`", idx)

	assert.Equal(t, "", result)
}

func TestAlterOptionEngine(t *testing.T) {
	g := NewMySQLGenerator()

	opt := &diff.TableOptionChange{Name: "ENGINE", New: "InnoDB"}

	result := g.alterOption("`users`", opt)

	assert.Equal(t, "ALTER TABLE `users` ENGINE=InnoDB;", result)
}

func TestAlterOptionAutoIncrement(t *testing.T) {
	g := NewMySQLGenerator()

	opt := &diff.TableOptionChange{Name: "AUTO_INCREMENT", New: "100"}

	result := g.alterOption("`users`", opt)

	assert.Equal(t, "ALTER TABLE `users` AUTO_INCREMENT=100;", result)
}

func TestAlterOptionCharset(t *testing.T) {
	g := NewMySQLGenerator()

	opt := &diff.TableOptionChange{Name: "CHARSET", New: "utf8mb4"}

	result := g.alterOption("`users`", opt)

	assert.Equal(t, "ALTER TABLE `users` DEFAULT CHARSET=utf8mb4;", result)
}

func TestAlterOptionCollate(t *testing.T) {
	g := NewMySQLGenerator()

	opt := &diff.TableOptionChange{Name: "COLLATE", New: "utf8mb4_unicode_ci"}

	result := g.alterOption("`users`", opt)

	assert.Equal(t, "ALTER TABLE `users` COLLATE=utf8mb4_unicode_ci;", result)
}

func TestAlterOptionComment(t *testing.T) {
	g := NewMySQLGenerator()

	opt := &diff.TableOptionChange{Name: "COMMENT", New: "User accounts"}

	result := g.alterOption("`users`", opt)

	assert.Equal(t, "ALTER TABLE `users` COMMENT='User accounts';", result)
}

func TestAlterOptionRowFormat(t *testing.T) {
	g := NewMySQLGenerator()

	opt := &diff.TableOptionChange{Name: "ROW_FORMAT", New: "DYNAMIC"}

	result := g.alterOption("`users`", opt)

	assert.Equal(t, "ALTER TABLE `users` ROW_FORMAT=DYNAMIC;", result)
}

func TestAlterOptionNumericValue(t *testing.T) {
	g := NewMySQLGenerator()

	opt := &diff.TableOptionChange{Name: "KEY_BLOCK_SIZE", New: "8"}

	result := g.alterOption("`users`", opt)

	assert.Equal(t, "ALTER TABLE `users` KEY_BLOCK_SIZE=8;", result)
}

func TestAlterOptionStringValue(t *testing.T) {
	g := NewMySQLGenerator()

	opt := &diff.TableOptionChange{Name: "CUSTOM_OPTION", New: "custom_value"}

	result := g.alterOption("`users`", opt)

	assert.Equal(t, "ALTER TABLE `users` CUSTOM_OPTION='custom_value';", result)
}

func TestAlterOptionEmptyValue(t *testing.T) {
	g := NewMySQLGenerator()

	opt := &diff.TableOptionChange{Name: "ENGINE", New: ""}

	result := g.alterOption("`users`", opt)

	assert.Equal(t, "", result)
}

func TestTableOptions(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{
		Name:    "users",
		Comment: "User accounts",
		Options: core.TableOptions{
			Engine:        "InnoDB",
			Charset:       "utf8mb4",
			Collate:       "utf8mb4_unicode_ci",
			AutoIncrement: 100,
			RowFormat:     "DYNAMIC",
			AvgRowLength:  256,
			KeyBlockSize:  8,
			MaxRows:       1000000,
			MinRows:       1,
			Compression:   "LZ4",
			Encryption:    "Y",
			Tablespace:    "my_tablespace",
		},
	}

	result := g.tableOptions(table)

	assert.Contains(t, result, "ENGINE=InnoDB")
	assert.Contains(t, result, "DEFAULT CHARSET=utf8mb4")
	assert.Contains(t, result, "COLLATE=utf8mb4_unicode_ci")
	assert.Contains(t, result, "AUTO_INCREMENT=100")
	assert.Contains(t, result, "ROW_FORMAT=DYNAMIC")
	assert.Contains(t, result, "AVG_ROW_LENGTH=256")
	assert.Contains(t, result, "KEY_BLOCK_SIZE=8")
	assert.Contains(t, result, "MAX_ROWS=1000000")
	assert.Contains(t, result, "MIN_ROWS=1")
	assert.Contains(t, result, "COMPRESSION='LZ4'")
	assert.Contains(t, result, "ENCRYPTION='Y'")
	assert.Contains(t, result, "TABLESPACE `my_tablespace`")
	assert.Contains(t, result, "COMMENT='User accounts'")
}

func TestTableOptionsEmpty(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{
		Name: "users",
	}

	result := g.tableOptions(table)

	assert.Equal(t, "", result)
}

func TestColumnDefinition(t *testing.T) {
	g := NewMySQLGenerator()

	col := &core.Column{
		Name:          "email",
		TypeRaw:       "VARCHAR(255)",
		Nullable:      false,
		AutoIncrement: false,
		Charset:       "utf8mb4",
		Collate:       "utf8mb4_unicode_ci",
		Comment:       "User email",
	}
	defVal := "test@example.com"
	col.DefaultValue = &defVal

	result := g.columnDefinition(col)

	assert.Contains(t, result, "`email`")
	assert.Contains(t, result, "VARCHAR(255)")
	assert.Contains(t, result, "NOT NULL")
	assert.Contains(t, result, "CHARACTER SET utf8mb4")
	assert.Contains(t, result, "COLLATE utf8mb4_unicode_ci")
	assert.Contains(t, result, "DEFAULT 'test@example.com'")
	assert.Contains(t, result, "COMMENT 'User email'")
}

func TestColumnDefinitionNullable(t *testing.T) {
	g := NewMySQLGenerator()

	col := &core.Column{
		Name:     "middle_name",
		TypeRaw:  "VARCHAR(100)",
		Nullable: true,
	}

	result := g.columnDefinition(col)

	assert.Contains(t, result, "NULL")
	assert.NotContains(t, result, "NOT NULL")
}

func TestColumnDefinitionAutoIncrement(t *testing.T) {
	g := NewMySQLGenerator()

	col := &core.Column{
		Name:          "id",
		TypeRaw:       "INT",
		Nullable:      false,
		AutoIncrement: true,
	}

	result := g.columnDefinition(col)

	assert.Contains(t, result, "AUTO_INCREMENT")
}

func TestColumnDefinitionAutoRandom(t *testing.T) {
	g := NewMySQLGenerator()

	col := &core.Column{
		Name:       "id",
		TypeRaw:    "BIGINT",
		Nullable:   false,
		AutoRandom: 5,
	}

	result := g.columnDefinition(col)

	assert.Contains(t, result, "AUTO_RANDOM(5)")
}

func TestColumnDefinitionGenerated(t *testing.T) {
	g := NewMySQLGenerator()

	col := &core.Column{
		Name:                 "full_name",
		TypeRaw:              "VARCHAR(255)",
		Nullable:             true,
		IsGenerated:          true,
		GenerationExpression: "CONCAT(first_name, ' ', last_name)",
		GenerationStorage:    "STORED",
	}

	result := g.columnDefinition(col)

	assert.Contains(t, result, "GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) STORED")
}

func TestColumnDefinitionGeneratedVirtual(t *testing.T) {
	g := NewMySQLGenerator()

	col := &core.Column{
		Name:                 "full_name",
		TypeRaw:              "VARCHAR(255)",
		Nullable:             true,
		IsGenerated:          true,
		GenerationExpression: "CONCAT(first_name, ' ', last_name)",
	}

	result := g.columnDefinition(col)

	assert.Contains(t, result, "GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) VIRTUAL")
}

func TestColumnDefinitionOnUpdate(t *testing.T) {
	g := NewMySQLGenerator()

	onUpdate := "CURRENT_TIMESTAMP"
	col := &core.Column{
		Name:     "updated_at",
		TypeRaw:  "TIMESTAMP",
		Nullable: false,
		OnUpdate: &onUpdate,
	}

	result := g.columnDefinition(col)

	assert.Contains(t, result, "ON UPDATE CURRENT_TIMESTAMP")
}

func TestColumnDefinitionColumnFormat(t *testing.T) {
	g := NewMySQLGenerator()

	col := &core.Column{
		Name:         "data",
		TypeRaw:      "VARCHAR(255)",
		Nullable:     true,
		ColumnFormat: "FIXED",
	}

	result := g.columnDefinition(col)

	assert.Contains(t, result, "COLUMN_FORMAT FIXED")
}

func TestColumnDefinitionStorage(t *testing.T) {
	g := NewMySQLGenerator()

	col := &core.Column{
		Name:     "data",
		TypeRaw:  "VARCHAR(255)",
		Nullable: true,
		Storage:  "DISK",
	}

	result := g.columnDefinition(col)

	assert.Contains(t, result, "STORAGE DISK")
}

func TestSupportsCharsetCollation(t *testing.T) {
	tests := []struct {
		typeRaw  string
		expected bool
	}{
		{"CHAR(10)", true},
		{"VARCHAR(255)", true},
		{"TINYTEXT", true},
		{"TEXT", true},
		{"MEDIUMTEXT", true},
		{"LONGTEXT", true},
		{"ENUM('a', 'b')", true},
		{"SET('a', 'b')", true},
		{"INT", false},
		{"BIGINT", false},
		{"DECIMAL(10,2)", false},
		{"BLOB", false},
		{"JSON", false},
		{"BINARY(16)", false},
		{"VARBINARY(255)", false},
	}

	for _, tt := range tests {
		t.Run(tt.typeRaw, func(t *testing.T) {
			result := supportsCharsetCollation(tt.typeRaw)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSupportsCharsetCollationEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		typeRaw  string
		expected bool
	}{
		{"empty string", "", false},
		{"only whitespace", "   ", false},
		{"starts with parenthesis", "(something)", false},
		{"starts with number", "123type", false},
		{"special chars only", "!@#$%", false},
		{"quoted identifier", "`varchar`", false},
		{"starts with dash", "-type", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := supportsCharsetCollation(tt.typeRaw)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeMySQLTypeRaw(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"VARCHAR(255)", "VARCHAR(255)"},
		{"VARBINARY(255) BINARY", "VARBINARY(255)"},
		{"BINARY(16) BINARY", "BINARY(16)"},
		{"INT", "INT"},
		{"", ""},
		{"  VARCHAR(255)  ", "VARCHAR(255)"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sanitizeMySQLTypeRaw(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeMySQLTypeRawEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"starts with parenthesis", "(something)", "(something)"},
		{"starts with number", "123type", "123type"},
		{"special chars only", "!@#$%", "!@#$%"},
		{"quoted identifier", "`varchar`", "`varchar`"},
		{"only whitespace", "   ", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeMySQLTypeRaw(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
