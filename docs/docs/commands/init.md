# smf init

The `init` command initializes a new `smf` project by creating a default `schema.toml` file in the current directory.

## Usage

```bash
smf init [flags]
```

## Flags

| Flag        | Shorthand | Description                              | Default       |
|:------------|:----------|:-----------------------------------------|:--------------|
| `--dialect` | `-d`      | Database dialect for the initial schema  | `mysql`       |
| `--output`  | `-o`      | Custom filename for the schema           | `schema.toml` |

## Example

```bash
smf init --dialect postgresql
```

This will create a `schema.toml` file with basic configuration for a PostgreSQL database.
