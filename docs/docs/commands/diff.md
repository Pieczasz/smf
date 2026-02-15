# smf diff

The `diff` command compares your current `schema.toml` with the state after the latest migration to show pending changes.

## Usage

```bash
smf diff [flags]
```

## Flags

| Flag               | Shorthand | Description                              | Default       |
|:-------------------|:----------|:-----------------------------------------|:--------------|
| `--schema`         | `-s`      | Path to the schema file                  | `schema.toml` |
| `--migrations-dir` | `-m`      | Directory where migrations are stored     | `./migrations`|
| `--format`         | `-f`      | Output format: `json` or `sql`           | `sql`         |

## Example

```bash
smf diff
```
