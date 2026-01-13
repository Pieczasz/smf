# SMF Roadmap (Developer-Oriented)

## Milestones

Each milestone is designed to be shippable and testable on its own.

### M0 — Docs + UX Polish

Goal: make the project usable by a new developer in 10 minutes.

- Update README with installation/build steps and "known limitations."
- Add examples that use `tests/fixtures/*.sql` as copy-pastable demo inputs.
- Define output stability guarantees (what is human-only vs. machine-parseable).

Acceptance:
- A user can run `smf diff` and `smf migrate` successfully using the fixtures.

### M1 — “Plan” output and deterministic formatting

Goal: produce a stable, reviewable migration plan for PRs.

- Add `smf plan <old.sql> <new.sql>`:
  - Includes summary counts (tables/columns/constraints/indexes changed)
  - Includes a breaking-change section (with severities)
  - Includes the generated SQL (same as `migrate`) optionally
- Add `--format` flag for all printing commands:
  - `--format=human` (default)
  - `--format=json` (CI friendly)

Acceptance:
- Golden tests for `--format=json` are stable across runs.

### M2 — Execution layer: `apply` (MySQL)

Goal: safely apply generated migrations to a live database.

- Add `smf apply --dsn <dsn> --file <migration.sql>`
- Add `--dry-run` that prints statements and exits non-zero if prechecks fail.
- Add `--transaction` behavior:
  - If a statement set is transaction-safe, run in a transaction.
  - If not transaction-safe, explain why and require `--allow-non-transactional`.
- Add “preflight checks”:
  - Warn when statements include potentially blocking DDL
  - Warn when statements are destructive and `--unsafe` was required

Acceptance:
- Integration tests behind an opt-in env var (e.g., `SMF_TEST_DSN`).

### M3 — Migration history and status

Goal: make production usage auditable.

- Create `_smf_migrations` table (id, name, checksum, applied_at, tool_version, user/host).
- Record one row per applied migration file.
- Add `smf status --dsn <dsn>`:
  - last applied migration
  - pending migration (if file provided)
- Add checksum verification:
  - refuse applying a migration file whose name exists with different checksum unless `--force`.

Acceptance:
- Tests validating checksum matching and history insert behavior.

### M4 — Rollback workflow

Goal: enable controlled rollback of the *last applied migration*.

- Add `smf rollback --dsn <dsn> --last`:
  - Uses rollback SQL stored/derived from the migration file
  - Records rollback event in history (or separate `_smf_rollbacks` table)
- Add `smf migrate --rollback-output` docs and ensure it is compatible with M2/M3.

Acceptance:
- For “safe mode” drops (renames to `__smf_backup_*`), rollback restores original names.

### M5 — PostgreSQL dialect

Goal: add a second dialect without breaking the existing MySQL pipeline.

- Implement `dialect/postgres`:
  - parser integration
  - generator integration
  - core-breaking-change rules (enums, serial/identity changes, type casts)
- Add fixture-based tests mirroring the MySQL suite.

Acceptance:
- `smf diff` works for Postgres dumps; `smf migrate` generates valid SQL for a subset.

### M6 — Advanced safety

- Expand-and-contract patterns (guided plan output)
- Online schema change hints (pt-osc / gh-ost / PG concurrent DDL)
- Data validation hooks (row counts, FK validation)