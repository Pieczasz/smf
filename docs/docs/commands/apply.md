# smf apply

The `apply` command connects to your database and applies all pending migrations from the migrations directory.

## Usage

```bash
smf apply [flags]
```

## Flags

| Flag                        | Shorthand | Description                                                 | Default      |
|:----------------------------|:----------|:------------------------------------------------------------|:-------------|
| `--dsn`                     |           | Database connection string (required)                       |              |
| `--migrations-dir`          | `-m`      | Directory where migrations are stored                       | `./migrations`|
| `--dry-run`                 | `-d`      | Print statements and run preflight checks without executing | `false`      |
| `--transaction`             | `-t`      | Run migration in a transaction if possible                  | `true`       |
| `--unsafe`                  | `-u`      | Allow destructive operations (DROP, TRUNCATE, etc.)         | `false`      |

## Preflight Checks

`smf apply` performs several safety checks before executing any SQL:
- **Destructive Operations**: Warns if any pending migration contains `DROP`, `TRUNCATE`, etc. Use `--unsafe` to proceed.
- **Transaction Safety**: Checks if migrations consist of statements that can be rolled back.

## Example

```bash
smf apply --dsn "root:password@tcp(localhost:3306)/mydb"
```
