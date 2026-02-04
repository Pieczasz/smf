# smf migrate

The `migrate` command generates the necessary SQL statements to transition a database schema from an old state to a new state.

## Usage

```bash
smf migrate <old.sql> <new.sql> [flags]
```

## Flags

| Flag | Shorthand | Description | Default |
| :--- | :--- | :--- | :--- |
| `--from` | | Source database dialect | `mysql` |
| `--to` | `-t` | Target database dialect | `mysql` |
| `--output` | `-o` | Output file for the generated migration SQL | (stdout) |
| `--rollback-output` | `-b` | Output file for generated rollback SQL | |
| `--format` | `-f` | Output format: `json` or `sql` | `sql` |
| `--unsafe` | `-u` | Generate unsafe migration (may drop/overwrite data) | `false` |
| `--detect-renames` | `-r` | Enable heuristic column rename detection | `true` |

## Example

```bash
smf migrate schema_v1.sql schema_v2.sql --output migration.sql --rollback-output rollback.sql
```
