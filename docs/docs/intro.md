---
sidebar_position: 1
---

# Introduction

`smf` is a database migration framework that uses a declarative TOML file to define your database schema. It bridges the gap between manual SQL migrations and full ORM-based schema management.

## Key Features

- **TOML-First**: Declare your tables, columns, and indexes in a readable `schema.toml`.
- **9 Dialects**: Seamlessly work with MySQL, MariaDB, PostgreSQL, SQLite, Oracle, DB2, Snowflake, MSSQL, and TiDB.
- **Automated Migrations**: Automatically generate timestamped SQL migrations by diffing your TOML schema against the previous state.
- **Safety Built-in**: `smf` detects potentially destructive changes and warns you before applying them.

## Core Commands

### `init`
Initialize your project with a `schema.toml` file.

### `diff`
Compare your current `schema.toml` with the last generated migration to see pending changes.

### `migrate`
Generate a new SQL migration file in the `migrations/` directory.

### `apply`
Apply all pending migrations to your target database.

## Quick Start

1. **Initialize**: `smf init`
2. **Define Schema**: Edit `schema.toml` to define your tables.
3. **Generate Migration**: `smf migrate --name init_schema`
4. **Apply**: `smf apply --dsn "..."`

