# Release Checklist

This is the pre-release gate for tagging a new version of MySEC. Every box must be
ticked before a release is cut. If any step fails, stop and fix — do not release on a
best-effort basis.

---

## 1. CI is green on `main`

- [ ] Latest commit on `main` shows all five CI jobs green:
  - `backend` (ruff, mypy, pytest)
  - `frontend` (lint, vitest, build)
  - `coverage-check` (all per-bucket floors met)
  - `smoke-install` (fresh-clone `make install && make verify && make test`)
  - `sec-schema-drift` (most recent weekly run, not skipped)

Check at: GitHub Actions → CI workflow → filter branch = `main`.

## 2. `make verify` passes locally on a fresh checkout

```bash
git clone <repo> /tmp/sec-adv-release-check
cd /tmp/sec-adv-release-check
cp .env.example .env
# Set a real SECRET_KEY
sed -i '' 's/^SECRET_KEY=.*/SECRET_KEY='"$(openssl rand -hex 32)"'/' .env
make install
make verify
make test
```

- [ ] `make install` returns within 5 minutes without error
- [ ] `make verify` shows `ok` on all five rows
- [ ] `make test` is green (no failures, no errors)

## 3. SEC schema drift probe ran in the last 7 days

- [ ] GitHub Actions → `sec-schema-drift` job → most recent successful run is ≤7 days old.
  If the scheduled run was skipped or the workflow is new, trigger a manual run via
  `gh workflow run ci.yml` (or the Actions UI).

Reason: if SEC changed column names or URL patterns between the last probe and now, the
monthly sync will silently fail in production. The probe catches drift early.

## 4. Coverage thresholds met

- [ ] `coverage-check` job on the release commit shows all buckets ≥ their floor:
  - `api/services/` ≥ 80%
  - `api/celery_tasks/` ≥ 80%
  - `api/routes/` ≥ 70%
  - `scripts/` ≥ 60%

- [ ] No bucket dropped compared to the previous release. If the number went down even
  while still above the floor, investigate before tagging.

## 5. Docs reflect any new config keys

If this release introduces new environment variables, Make targets, or Celery tasks:

- [ ] `.env.example` lists every new required env var with a sensible default or comment
- [ ] `README.md` Section 9 (Environment Variables Reference) is updated
- [ ] `README.md` Section 11 (Make Targets Reference) is updated for any new targets
- [ ] `CLAUDE.md` is updated if the change affects a documented invariant (distribution,
      Celery task contract, bulk import rules, test conventions, idempotency list)
- [ ] If the entrypoint's migrate/seed block changed, the "Distribution invariants"
      section in CLAUDE.md reflects it

## 6. Dead-letter queue is empty in your staging env

- [ ] `make dlq-inspect` on staging/dev shows no leftover failed tasks from the previous
      release. A non-empty DLQ either indicates unresolved bugs or masks real issues in
      the release candidate.

## 7. Manual smoke flows on a fresh install

Spin up a fresh environment (`make install` on a clean host or in a VM) and confirm:

- [ ] Frontend loads at `http://localhost:5173` with empty states on every page
- [ ] Create a platform via the UI → verify it appears in `/api/platforms`
- [ ] Run the bulk match upload with a small CSV (`name,city,state` columns, ~10 rows) →
      results render in the table
- [ ] Trigger a monthly sync (`curl -X POST http://localhost:8000/api/sync/trigger`) →
      watch Sync Dashboard; manifest rows transition `pending → processing → complete`

## 8. Tag and release

Only once every box above is ticked:

```bash
git tag -a vX.Y.Z -m "Release X.Y.Z"
git push origin vX.Y.Z
```

Draft a GitHub release linking the CI run, `sec-schema-drift` run, and coverage summary
from this checklist.
