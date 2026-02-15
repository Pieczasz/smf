# smf

`smf` is a lightweight, TOML-first database schema management framework. It allows you to declare your database schema in a simple `schema.toml` file and manage migrations across multiple SQL dialects.

## Core Philosophy

- **Declarative**: Define your target state in TOML, not through a series of manual SQL scripts.
- **Migration-based**: Generate versioned SQL migrations that you can review and apply.
- **Multi-dialect**: Support for 9 major SQL dialects.
- **Safe**: Built-in safety checks for destructive operations.

## Supported Dialects

`smf` supports the following SQL dialects:

- MySQL
- MariaDB
- PostgreSQL
- SQLite
- Oracle
- DB2
- Snowflake
- Microsoft SQL Server (MSSQL)
- TiDB

## Installation
<!--TODO-->

## Commands

### `init`
Initialize a new `schema.toml` file in the current directory.

```bash
smf init "schema_name"
```

### `diff`
See the difference between your current `schema.toml` and the previously migrated state.

```bash
smf diff
```

### `migrate`
Generate a new migration file based on changes in `schema.toml`. Migration files are timestamped (e.g., `20260215123000_schema.sql`).

```bash
smf migrate
```

### `apply`
Apply pending migrations to your database.

```bash
smf apply --dsn "user:pass@tcp(localhost:3306)/dbname"
```

## Project Structure

A typical `smf` project looks like this:

```text
.
├── schema.toml         # Your declarative schema definition
└── migrations/         # Generated SQL migrations
    ├── 20260215100000_schema_name.sql
    └── 20260215110000_schema_name.sql
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
