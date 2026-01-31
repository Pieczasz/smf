# smf Roadmap

## M0 - Onboarding & Documentation

**Goal:** New developers can use smf in 10 minutes without external docs.

**Tasks:**
- Add installation and quick start to README
- Include "known limitations" section  
- Create copy-paste examples using test fixtures
- Improve error message formatting and clarity
- Add help text to all CLI commands

---

## M1 - Plan Output & Deterministic Formatting

**Problem:** Schema change plans need to be stable for CI/CD and reviewable in PRs.

**Current Issues:**
- SQL output changes formatting between runs
- JSON is difficult to review in pull requests
- No risk assessment or change summary

### Deliverables

#### 1. New Command: `smf plan`

```bash
$ smf plan old.sql new.sql

Summary:
  Tables:    3 added, 2 modified, 0 dropped
  Columns:   5 added, 2 renamed, 1 modified, 0 dropped
  Indexes:   2 added, 0 dropped
  Constraints: 1 new FK

Breaking Changes (2):
  - posts.content: TYPE CHANGE TEXT -> VARCHAR(100)
    Risk: CRITICAL
  - users.status: NULL -> NOT NULL change
    Risk: BREAKING - requires backfill

Safe Changes (6):
  - users.id: AUTO_INCREMENT added
  - users.nickname: ADD COLUMN
  - posts.likes_count: ADD COLUMN with DEFAULT
```

#### 2. Format Flags

```bash
# SQL-readable (default)
$ smf plan old.sql new.sql --format=sql

# JSON (stable for CI/CD)
$ smf plan old.sql new.sql --format=json

# Diff only (quick check)
$ smf plan old.sql new.sql --format=diff-only
Tables:    +3, ~2, -0
Columns:   +5, ~2, -0
```

#### 3. Risk Levels

Every operation gets tagged with one of these:
- **INFO:** Safe, no concerns
- **WARNING:** Requires review
- **BREAKING:** Requires pre-migration work
- **CRITICAL:** Potential data loss

### Implementation

1. Extract diff summary logic from `internal/diff` -> reusable module
2. Create `internal/plan/` package wrapping diff + migration generation
3. Add `--format` flag to output handler
4. Tag every `core.Operation` with risk level

### Acceptance Criteria

- `smf plan` output is identical across multiple runs (deterministic)
- JSON includes `plan_format_version` header
- Every breaking change includes a `suggestion` field
- `smf plan --breaking-only` exits with code 1 if changes found (CI/CD gate)

---

## M2 - Execution Layer: Apply Command (MySQL)

**Problem:** Running schema changes in production is scary without preflight checks and dry-run support.

**Current Issues:**
- Developers manually copy-paste SQL (typo-prone)
- No validation of foreign keys before applying
- No awareness of table lock duration
- No audit trail of what was applied

### Deliverables

#### 1. Basic Apply Command

```bash
$ smf apply --dsn "mysql://user:pass@localhost:3306/db" --file migration.sql

Preflight checks...
  OK: Database is accessible
  OK: All migrations are valid SQL
  OK: No FK violations detected
  WARNING: ALTER TABLE users will lock for ~5s

Statements to execute:
  1. ALTER TABLE users ADD COLUMN subscription_tier ENUM('free','pro') DEFAULT 'free';
  2. ALTER TABLE posts ADD COLUMN likes_count INT DEFAULT 0;

Execute? [y/n]: y

Executing...
  [1/2] OK: ALTER TABLE users ... (0.12s)
  [2/2] OK: ALTER TABLE posts ... (0.08s)

Migration complete!
```

#### 2. Dry-Run Mode

```bash
$ smf apply --dsn "mysql://..." --file migration.sql --dry-run

Preflight checks...
  OK: Database is accessible
  OK: All migrations are valid SQL
  OK: No FK violations detected

Statements (DRY RUN - not executed):
  1. ALTER TABLE users ADD COLUMN subscription_tier ...
  2. ALTER TABLE posts ADD COLUMN likes_count ...

Would execute 2 statements (estimated time: 0.20s)
Exit code: 0 (safe to execute)
```

#### 3. Preflight Checks

- Database connectivity
- User has ALTER privilege on all tables
- No FK violations from column type changes
- No missing indexes
- Table isn't already locked

#### 4. Transaction Safety

- Identify which statements are transaction-safe
- Require `--allow-non-transactional` for unsafe statements
- Suggest splitting migrations if needed

#### 5. Lock Time Estimation

- Use MySQL INFORMATION_SCHEMA to estimate lock duration
- Warn if operation will lock for > 5 seconds
- Suggest tools like `pt-online-schema-change` for large tables

### Implementation

1. Extend `internal/apply/` with execution logic
2. Add `internal/apply/preflight.go` for validation
3. Add `internal/apply/estimate.go` for lock time estimation
4. Add `internal/apply/transaction_planner.go` for safety analysis

### Acceptance Criteria

- `smf apply --dry-run` exits 0 (safe) or 1 (has issues)
- Integration tests with real MySQL instance
- Preflight checks prevent 80% of common mistakes
- Lock time estimates are within ±2 seconds

---

## M3 - Migration History & Status

**Problem:** No way to track applied migrations or prevent re-applying the same migration.

### Deliverables

#### 1. Migration History Table

```sql
CREATE TABLE _smf_migrations (
  id INT PRIMARY KEY AUTO_INCREMENT,
  migration_name VARCHAR(255) NOT NULL UNIQUE,
  checksum VARCHAR(64) NOT NULL,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  duration_ms INT,
  applied_by VARCHAR(100),
  applied_from_host VARCHAR(100),
  status ENUM('success', 'rolled_back') DEFAULT 'success'
);
```

#### 2. Checksum Validation

Before applying, check if migration was already applied:

```bash
$ smf apply --dsn "mysql://..." --file migration_1_add_users.sql

Checking migration history...
  Found: migration_1_add_users.sql (applied 2026-01-20 16:32:00)
  Checksums match (OK)

Skip? [y/n]: y
```

If file was edited after being applied, fail with an error.

#### 3. Status Command

```bash
$ smf status --dsn "mysql://..."

Last Applied:
  migration_3_add_posts_table.sql (2026-01-20 16:45:30)
  Duration: 0.45s

Pending Migrations:
  (None - schema is up to date)
```

#### 4. History Command

```bash
$ smf history --dsn "mysql://..."

History of Applied Migrations:

1. migration_1_add_users_table.sql
   Applied: 2026-01-15 10:30:00
   Duration: 0.12s
   Applied by: bob@localhost

2. migration_2_add_posts_table.sql
   Applied: 2026-01-17 14:20:00
   Duration: 0.08s
   Applied by: alice@localhost
```

### Implementation

1. Create `internal/history/` module to manage `_smf_migrations` table
2. Add checksum verification to apply flow
3. Add `smf status` command
4. Add `smf history` command

### Acceptance Criteria

- `_smf_migrations` table created on first apply
- Checksum mismatch blocks re-apply (unless `--force`)
- `smf status` shows last applied migration
- Each migration logged with metadata (user, host, duration)

---

## M4 - Rollback Workflow

**Problem:** After applying a bad migration, there's no safe way to undo it.

### Deliverables

#### 1. Rollback Command

```bash
$ smf rollback --dsn "mysql://..." --last

Checking rollback safety...
  Last migration: migration_3_add_posts_table.sql
  Rollback SQL available: YES

Rollback will execute:
  ALTER TABLE posts DROP COLUMN content;
  ALTER TABLE posts RENAME TO __smf_backup_posts;

Proceed? [y/n]: y

Executing rollback...
  [1/2] OK: ALTER TABLE posts DROP COLUMN content; (0.08s)
  [2/2] OK: ALTER TABLE posts RENAME TO __smf_backup_posts; (0.02s)

Rollback complete!
Original table backed up as: __smf_backup_posts
```

#### 2. Dry-Run Preview

```bash
$ smf rollback --dsn "mysql://..." --last --dry-run

Preview of rollback:
  Last migration: migration_3_add_posts_table.sql (applied 2 hours ago)

Rollback will execute:
  ALTER TABLE posts CHANGE COLUMN __smf_backup_content content LONGTEXT;

Exit code: 0 (safe to execute)
```

#### 3. Safe Mode Rollbacks

Use backup columns instead of dropping data:

```bash
# Forward:
ALTER TABLE posts ADD COLUMN content_new LONGTEXT;

# Rollback (restores original):
ALTER TABLE posts DROP COLUMN content_new;
```

### Implementation

1. Store rollback SQL in history table
2. Add `smf rollback` command with `--last` flag
3. Add `_smf_rollbacks` table for audit trail
4. Add `--dry-run` support

### Acceptance Criteria

- `smf rollback --last --dry-run` shows what will be rolled back
- Safe mode rollbacks restore data
- Rollback is recorded in history
- Can only rollback the last migration

---

## M5 - PostgreSQL Dialect

**Problem:** smf only works with MySQL. PostgreSQL users are left out.

### Deliverables

#### 1. PostgreSQL Parser & Generation

Handle PostgreSQL-specific syntax:
- SERIAL / BIGSERIAL
- ENUM types
- GENERATED columns
- Array types

#### 2. PostgreSQL-Specific Operations

```bash
$ smf plan old.sql new.sql --dialect=postgres

PostgreSQL-Specific Changes:
  - users.id: SERIAL -> BIGSERIAL (safe)
  - users.status: VARCHAR(50) -> ENUM (requires CREATE TYPE + USING clause)

Safe Changes:
  - users.email: ADD COLUMN (can use CONCURRENT)
```

#### 3. ENUM Type Handling

```bash
# Forward:
CREATE TYPE user_status AS ENUM('active', 'inactive');
ALTER TABLE users ALTER COLUMN status TYPE user_status USING status::user_status;

# Rollback:
ALTER TABLE users ALTER COLUMN status TYPE VARCHAR(50);
DROP TYPE user_status;
```

#### 4. Concurrent Operations

Suggest non-blocking syntax for large tables:

```bash
CREATE INDEX CONCURRENTLY idx_user_status ON users(status);
```

### Implementation

1. Copy `internal/dialect/mysql/` -> `internal/dialect/postgres/`
2. Implement PostgreSQL SQL generation
3. Add `--dialect` flag to CLI
4. Add PostgreSQL fixture tests

### Acceptance Criteria

- `smf plan --dialect=postgres` produces valid PostgreSQL SQL
- ENUM migrations handled correctly
- Index creation suggests CONCURRENT syntax
- Fixture tests pass

---

## M6 - Advanced Safety (Expand-and-Contract & Validation)

**Problem:** Large table migrations lock for too long. Need zero-downtime patterns and data validation.

### Deliverables

#### 1. Expand-and-Contract Pattern

Suggest multi-phase migrations for zero-downtime:

```bash
$ smf plan old.sql new.sql --output-strategy=expand-and-contract

Recommended Strategy: Expand-and-Contract

Phase 1: Expand (backwards-compatible)
  ALTER TABLE users ADD COLUMN status_new VARCHAR(50);
  CREATE TRIGGER users_copy_status BEFORE INSERT ON users
    FOR EACH ROW SET NEW.status_new = NEW.status;
  UPDATE users SET status_new = status; -- can be async

Phase 2: Contract (fast, no lock)
  DROP TRIGGER users_copy_status;
  ALTER TABLE users DROP COLUMN status;
  ALTER TABLE users RENAME COLUMN status_new TO status;

Time: Phase 1: 2-5 minutes, Phase 2: <100ms
```

#### 2. Large Table Recommendations

```bash
$ smf plan old.sql new.sql --large-table-threshold=1000000

Large Table Detected:
  posts (8.2M rows): ALTER TABLE posts ADD COLUMN likes_count INT

Recommendations:
  1. pt-online-schema-change (Percona)
  2. gh-ost (GitHub)
  3. Native MySQL 8.0+ INSTANT algorithm
```

#### 3. Row Count Validation

```bash
$ smf apply --dsn "mysql://..." --file migration.sql --validate-row-counts

Capturing row counts before migration...
  users: 1,000,000 rows
  posts: 5,234,123 rows

Executing migration...

Validating row counts after migration...
  users: OK 1,000,000 rows (no data loss)
  posts: OK 5,234,123 rows (no data loss)
```

#### 4. Foreign Key Validation

```bash
$ smf apply --dsn "mysql://..." --file migration.sql --validate-fks

Validating foreign keys before migration...
  posts.user_id -> users.id: OK

Executing migration...

Validating foreign keys after migration...
  posts.user_id -> users.id: OK
  (new) comments.post_id -> posts.id: OK
```

### Implementation

1. Create `internal/strategies/` for expand-and-contract logic
2. Add `--output-strategy` flag
3. Create `internal/validate/` for row count and FK validation
4. Add `smf apply --validate-*` flags
5. Create `internal/recommend/` for tool suggestions

### Acceptance Criteria

- Expand-and-contract phases are independently deployable
- Row count validation catches data loss
- FK validation catches broken references
- Tool recommendations are accurate

---

## M7 - Schema Merge (Git Conflict Resolution)

**Problem:** When two developers modify the same table in different branches, git merge conflicts require manual resolution with no validation.

### Deliverables

#### 1. Merge Command

```bash
$ smf merge --base=main --branch-a=feature/subs --branch-b=feature/verification \
  --old-schema=schema_v1.5.sql

Analyzing conflicts...

Detected Changes:
  Branch A: + users.subscription_tier ENUM
  Branch B: + users.verified_at TIMESTAMP

No conflicts! Merging schemas...

Generated Merged Schema:
  ALTER TABLE users 
    ADD COLUMN subscription_tier ENUM('free','pro'),
    ADD COLUMN verified_at TIMESTAMP NULL;
```

#### 2. Conflict Detection

```bash
$ smf merge --base=main --branch-a=feature/rename --branch-b=feature/other

CONFLICT: Both branches modify users.id
  Branch A: RENAME users.id -> users.user_id
  Branch B: CHANGE users.id BIGINT -> INT

Choose resolution:
  1. Take Branch A
  2. Take Branch B
  3. Manual resolution

Option: 1
```

#### 3. CI/CD Integration

```yaml
on: [pull_request]

jobs:
  check-schema:
    runs-on: ubuntu-latest
    steps:
      - run: |
          smf merge \
            --base=$(git merge-base HEAD main) \
            --branch-a=HEAD \
            --branch-b=main \
            --old-schema=schema_v1.5.sql > /tmp/merged.sql
          
          if [ $? -ne 0 ]; then
            echo "Schema merge failed"
            exit 1
          fi
```

### Implementation

1. Create `internal/merge/` module
2. Implement 3-way diff logic
3. Detect conflicts (both branches modify same column)
4. Auto-merge safe changes
5. Report conflicts clearly

### Acceptance Criteria

- Non-conflicting changes auto-merge
- Conflicts detected and reported clearly
- Merged schema is validated
- CI/CD integration works

---

## M8 - Smart Rename Detection & Explicit Intent

**Problem:** Rename detection produces false positives. Need explicit intent declaration and better heuristics.

### Deliverables

#### 1. Explicit Intent File

```yaml
# migration.intent.yaml
tables:
  users:
    columns:
      - rename: { from: user_id, to: owner_id }
      - add: { name: nickname, type: VARCHAR(255) }
```

```bash
$ smf diff old.sql new.sql --intent=migration.intent.yaml

Changes:
  OK: users.user_id -> users.owner_id (RENAME, explicit)
  OK: users.nickname (ADD)
```

#### 2. Confidence Scoring

```bash
$ smf diff old.sql new.sql

Ambiguous Rename Detected:
  users.status -> users.state?
  Confidence: 42%
  
  Possible matches:
    1. status -> state (42%)
    2. status -> new_status (15%)
  
  Hint: Use --rename=status:state or declare in intent file
```

#### 3. FK-Aware Detection

```bash
$ smf diff old.sql new.sql

Smart Rename Detection:
  users.user_id -> users.owner_id (99% confidence)
    Reason: Foreign key from posts.user_id + semantic match
```

#### 4. Interactive Resolution

```bash
$ smf diff old.sql new.sql --interactive

Ambiguous rename detected:
  Removed: status, state, status_flag
  Added: user_status, account_status, approved

Match old columns to new columns:
  1. status -> ? [a) user_status (42%), b) account_status (35%)]
     Select: a
  2. state -> ? [a) account_status (65%), b) user_status (28%)]
     Select: a
  3. status_flag -> ? [a) approved (78%)]
     Select: a

Generated: migration.intent.yaml
```

### Implementation

1. Create `internal/intent/` for YAML parsing
2. Enhance rename detection with FK awareness
3. Add confidence scoring
4. Add `--intent`, `--rename`, `--no-infer-renames`, `--interactive-rename` flags

### Acceptance Criteria

- Explicit renames override heuristics
- FK-aware detection >90% accurate
- Confidence scores reported
- Low-confidence renames flagged for review

---

## M9 - Policy Enforcement

**Problem:** Teams need to enforce schema change rules (e.g., no immediate column drops, require review for breaking changes).

### Deliverables

#### 1. Policy File Format

```yaml
# .smf/policies.yaml

policies:
  - name: "no-immediate-drop"
    type: BREAKING
    severity: ERROR
    on:
      - operation: DROP_COLUMN
      - operation: DROP_TABLE
    then:
      - block: true
        reason: "Destructive operations require 30-day grace period"
        suggest: |
          Instead of dropping immediately:
          1. Rename to __smf_deprecated_{column_name}
          2. Update application to ignore
          3. Drop after 30 days

  - name: "type-change-requires-approval"
    type: WARNING
    severity: WARNING
    on:
      - operation: MODIFY_COLUMN
        where: "type change"
    then:
      - require-approval: true
        reviewers: ["@db-team"]

  - name: "large-table-warning"
    type: WARNING
    severity: INFO
    on:
      - operation: ALTER_TABLE
        where: "row_count > 1000000"
    then:
      - warn: true
        suggest: "Consider pt-online-schema-change or gh-ost"
```

#### 2. Validation Command

```bash
$ smf validate --file migration.sql --policies .smf/policies.yaml

Validating against policies...

ERROR: "no-immediate-drop"
  Operation: DROP TABLE posts
  Reason: Destructive operations require 30-day grace period
  Suggestion: Rename to __smf_deprecated_posts first

WARNING: "large-table-warning"
  Operation: ALTER TABLE users (1.2M rows)
  Reason: Large table detected
  Suggestion: Consider pt-online-schema-change

1 error, 1 warning
Exit code: 1
```

#### 3. Override Mechanism

```bash
# In migration file:
ALTER TABLE posts DROP TABLE posts; 
/* smf-override: no-immediate-drop, approved-by: @alice, expires: 2026-02-20 */

# In CLI:
smf apply --dsn "mysql://..." --file migration.sql --override=no-immediate-drop

# In git message:
$ git commit -m "Drop old posts table

This overrides 'no-immediate-drop' because table is no longer used.

smf-override: no-immediate-drop
approved-by: @alice
"

$ smf validate --from-git-message
```

#### 4. CI/CD Integration

```yaml
on: [pull_request]

jobs:
  check-policies:
    runs-on: ubuntu-latest
    steps:
      - run: |
          smf validate \
            --file migration.sql \
            --policies .smf/policies.yaml
          
          # Blocks merge if policies fail
```

### Implementation

1. Create `internal/policy/` module
2. Parse YAML policy file
3. Implement policy evaluator
4. Add `smf validate` command with `--policies` flag
5. Support override mechanisms

### Acceptance Criteria

- Policies evaluated for every operation
- Violations reported with clear reasons
- Override mechanism is auditable
- CI/CD integration blocks unsafe changes

---

## M10 - Observability & Metrics (Phase 2)

**Problem:** No visibility into how schemas are changing over time or what the safety metrics are.

### Deliverables

#### 1. Metrics Command

```bash
$ smf metrics --dsn "mysql://..." --from=2026-01-01 --to=2026-01-31

January 2026 Schema Metrics

Migrations:
  Total: 24
  Success: 23
  Rolled back: 1
  Failed: 0

Distribution:
  ADD COLUMN: 12 (50%)
  CREATE INDEX: 7 (29%)
  DROP COLUMN: 3 (13%)
  MODIFY COLUMN: 2 (8%)

Performance:
  Average duration: 0.45s
  Longest: ALTER TABLE posts ADD INDEX (2.3s)
  Shortest: ADD COLUMN (0.02s)

Safety:
  Rollbacks: 1 (4% rate)
  Data loss incidents: 0
  FK violations: 0

Top Contributors:
  alice: 12 migrations
  bob: 8 migrations
  charlie: 4 migrations
```

#### 2. Export Formats

```bash
# JSON output
$ smf metrics --dsn "mysql://..." --format=json > metrics.json

# Prometheus format
$ smf metrics --dsn "mysql://..." --format=prometheus
smf_migrations_total{status="success"} 23
smf_migrations_total{status="rolled_back"} 1
smf_migration_duration_seconds_bucket{le="1"} 18
smf_migration_duration_seconds_bucket{le="5"} 23
```

#### 3. Alerts

```yaml
# .smf/metrics.yaml
alerts:
  - name: "high-rollback-rate"
    threshold: "rollback_rate > 5%"
    action: "Notify @db-team on Slack"
  
  - name: "slow-migration"
    threshold: "migration_duration > 5s"
    action: "Warn in PR comment"
```

### Implementation

1. Create `internal/metrics/` module
2. Query `_smf_migrations` and `_smf_rollbacks` tables
3. Generate summary statistics
4. Add `smf metrics` command
5. Support JSON and Prometheus export

### Acceptance Criteria

- Metrics accurately reflect migration history
- Export formats are stable and parseable
- Performance metrics match actual execution

---

## MVP (Minimum Viable Product)

Core milestones needed to launch:
- M0: Onboarding & docs
- M1: Plan output
- M2: Apply command
- M3: History & status

At this stage: safe, reviewable migrations with execution and audit trail.

---

## Success Criteria by Milestone

| Milestone | Definition of Success |
|-----------|----------------------|
| M0 | New dev can use smf in <10 minutes |
| M1 | Plan output is identical across runs |
| M2 | 95% of preflight checks prevent errors |
| M3 | No re-applied migrations (checksum validation works) |
| M4 | Rollback restores data fully |
| M5 | PostgreSQL fixtures pass tests |
| M6 | Zero-downtime patterns are accurate |
| M7 | 80%+ merge conflicts auto-resolve |
| M8 | FK-aware renames >90% accurate |
| M9 | Policies prevent 95% of common mistakes |
| M10 | Metrics match actual history |
