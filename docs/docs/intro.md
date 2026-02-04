---
sidebar_position: 1
---

# Introduction

`smf` is a database migration tool for MySQL designed to help you manage your database schema efficiently. It provides tools to compare schemas, generate migrations, and apply them with safety checks.

## Key Features

- **Schema Comparison**: Compare two SQL schema dumps and see the differences.
- **Migration Generation**: Automatically generate the SQL statements needed to transition from one schema state to another.
- **Safe Execution**: Preflight checks warn about destructive operations (like `DROP TABLE` or `TRUNCATE`) and transaction-safety issues.
- **Dialect Support**: Currently focused on **MySQL**.

## Quick Start

To see what `smf` can do, you can run the following commands:

### Compare two schemas

```bash
smf diff old_schema.sql new_schema.sql
```

### Generate a migration

```bash
smf migrate old_schema.sql new_schema.sql --output migration.sql
```

### Apply a migration

```bash
smf apply --dsn "user:pass@tcp(localhost:3306)/dbname" --file migration.sql
```

## Installation

Building from source:

```bash
go build -o smf ./cmd/smf
```

