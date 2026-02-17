package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDatabaseTimestampsValidation(t *testing.T) {
	d := DialectMySQL

	t.Run("disabled timestamps", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:       "users",
					Columns:    []*Column{{Name: "id", Type: DataTypeInt}},
					Timestamps: &TimestampsConfig{Enabled: false},
				},
			},
		}

		err := db.Validate()
		require.NoError(t, err)
	})

	t.Run("default created and updated names are distinct", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:       "users",
					Columns:    []*Column{{Name: "id", Type: DataTypeInt}},
					Timestamps: &TimestampsConfig{Enabled: true},
				},
			},
		}

		err := db.Validate()
		require.NoError(t, err)
	})

	t.Run("same timestamps names", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:    "users",
					Columns: []*Column{{Name: "id", Type: DataTypeInt}},
					Timestamps: &TimestampsConfig{
						Enabled:       true,
						CreatedColumn: "created_at",
						UpdatedColumn: "created_at",
					},
				},
			},
		}

		err := db.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolve to the same name")
	})
}
