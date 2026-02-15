# smf diff

The `diff` command compares two SQL schema files and outputs the differences.

## Usage

```bash
smf diff <old.sql> <new.sql> [flags]
```

## Flags

| Flag               | Shorthand | Description                              | Default  |
|:-------------------|:----------|:-----------------------------------------|:---------|
| `--output`         | `-o`      | Output file for the diff                 | (stdout) |
| `--format`         | `-f`      | Output format: `json` or `sql`           | `sql`    |
| `--detect-renames` | `-r`      | Enable heuristic column rename detection | `true`   |
| `--dialect`        |           | Database dialect (e.g., `mysql`)         | `mysql`  |

## Example

```bash
smf diff schema_v1.sql schema_v2.sql --format json
```
