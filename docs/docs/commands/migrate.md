# smf migrate

The `migrate` command generates a new SQL migration file by comparing your `schema.toml` against the previous migrated state.

## Usage

```bash
smf migrate [flags]
```

## Flags

| Flag               | Shorthand | Description                                         | Default       |
|:-------------------|:----------|:----------------------------------------------------|:--------------|
| `--name`           | `-n`      | Descriptive name for the migration                  | `schema`      |
| `--schema`         | `-s`      | Path to the schema file                             | `schema.toml` |
| `--migrations-dir` | `-m`      | Directory to save the migration                     | `./migrations`|
| `--unsafe`         | `-u`      | Generate unsafe migration (may drop/overwrite data) | `false`       |

## Example

```bash
smf migrate --name add_users_table
```

This will create a file like `migrations/20260215123000_add_users_table.sql`.
