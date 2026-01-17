# SMF

SMF is a lightweight CLI for diffing database schemas from SQL dumps and generating (SQL) migration with breaking-change warnings and rollback.

It is intentionally config-free: you give it two schema files (`old.sql`, `new.sql`), it shows what changed and produces a migration script.

## Status

- Supported dialects: MySQL (diff + migration generation)
- Execution: not implemented yet (SMF generates SQL but does not run it)

## Build

This repository currently uses a local Go module name (`module smf`), so the simplest workflow is building from source:

```bash
go build -o smf ./
```

Run directly:

```bash
go run . --help
```

Run tests:

```bash
go test ./...
```

## CLI

### Diff two schemas

```bash
./smf diff <old.sql> <new.sql>
./smf diff <old.sql> <new.sql> -o diff.txt
```

Tip: there are ready-to-use fixture, so you can test the behavior:

```bash
./smf diff test/data/mysql_schema_v1.sql test/data/mysql_schema_v2.sql
```

### Generate a migration (MySQL)

```bash
./smf migrate <old.sql> <new.sql>
./smf migrate <old.sql> <new.sql> -o migration.sql
```

By default, SMF runs in safe mode (non-destructive where possible):

- dropped tables are renamed to `__smf_backup_*` instead of `DROP TABLE`
- dropped columns are renamed to `__smf_backup_*` instead of `DROP COLUMN`

To allow destructive changes, pass `--unsafe`:

```bash
./smf migrate <old.sql> <new.sql> --unsafe -o migration.sql
```

### Generate rollback SQL

SMF can emit rollback SQL for the generated migration (to run separately):

```bash
./smf migrate <old.sql> <new.sql> -o migration.sql -r rollback.sql
```

Rollback generation is "best-effort". For example, a true `DROP TABLE` cannot be automatically restored without an external backup. Maybe we will do something for it later.

## What SMF detects today

The generator annotates output with warnings/breaking changes for schema operations such as:

- table/column drops (data loss)
- type changes (widening vs narrowing) and risky length shrinks
- `NULL` -> `NOT NULL` transitions and adding `NOT NULL` columns without defaults
- constraint/index additions/removals/modifications (PK/UK/FK/CHECK)
- charset/collation changes (table + column)
- heuristics for likely column renames (to preserve data via `CHANGE COLUMN`)

## Known limitations

- SMF operates on SQL schema dumps (DDL). It does not inspect live DB data.
- SMF does not apply migrations to a database yet (no `apply`, no `_smf_migrations` table).
- Output is currently optimized for humans; stable JSON output is planned.

## Contributing

- Core diff logic lives under `diff/`.
- Dialect-specific generation lives under `dialect/` (MySQL implementation in `dialect/mysql/`).
- Parser integration lives under `parser/`.

If you add new behavior, please add/extend tests under `tests/` and prefer fixture-based regression tests.
