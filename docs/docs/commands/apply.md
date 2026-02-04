# smf apply

The `apply` command connects to your database and applies a migration file. It performs preflight checks to ensure safety.

## Usage

```bash
smf apply [flags]
```

## Flags

| Flag | Shorthand | Description | Default |
| :--- | :--- | :--- | :--- |
| `--dsn` | | Database connection string (required) | |
| `--file` | `-f` | Path to migration SQL file (required) | |
| `--dry-run` | `-d` | Print statements and run preflight checks without executing | `false` |
| `--transaction` | `-t` | Run migration in a transaction if possible | `true` |
| `--allow-non-transactional` | | Allow non-transactional DDL when `--transaction` is set | `false` |
| `--unsafe` | `-u` | Allow destructive operations (DROP, TRUNCATE, etc.) | `false` |
| `--timeout` | | Connection timeout in seconds | `300` |

## Preflight Checks

`smf apply` performs several safety checks before executing any SQL:
- **Destructive Operations**: Warns if the migration contains `DROP`, `TRUNCATE`, etc. Use `--unsafe` to proceed.
- **Transaction Safety**: Checks if the migration consists of statements that can be rolled back. MySQL DDL statements cause implicit commits and cannot be rolled back.

## Example

```bash
smf apply --dsn "root:password@tcp(localhost:3306)/mydb" --file migration.sql
```
