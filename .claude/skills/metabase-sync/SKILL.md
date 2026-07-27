---
name: metabase-sync
description: Version Metabase content in git via Enterprise remote sync — configure it, export, import, check for drift. Triggers — "put Metabase in git", "version control my dashboards", "sync Metabase content", "export the transforms", "is Metabase in sync with the repo?".
allowed-tools: Bash, Read, AskUserQuestion
---

# Versioning Metabase content

Metabase Enterprise can serialize its own content — transforms, cards,
dashboards, metrics — into a git repository. This is separate from, and
complementary to, the transform SQL that already lives in this repo: the SQL is
the *source*, the sync repo is Metabase's *serialized state*.

```bash
make mb-sync
```

`mbx gitsync` is a clean no-op when unconfigured. That is the default and it
blocks nothing.

## Turning it on

Needs two things from the user: a dedicated GitHub repository and a personal
access token with write access to it.

**Use a dedicated repo, not a branch of this one.** Metabase's serializer owns
its entire working tree, and its automated export commits should not interleave
with code history. Suggested name: `data-sovereignty-metabase-content`.

Then set in `.env` and run `make mb-sync`:

```
MB_GIT_SYNC_URL=https://github.com/<owner>/data-sovereignty-metabase-content.git
MB_GIT_SYNC_BRANCH=main
MB_GIT_SYNC_PAT=<token>
```

Requires the `remote_sync` license feature. Check with `make mb-audit` first.

## Order of operations

Remote sync starts read-only, and `add-collection` is rejected while it is. The
sequence is: set the URL, branch and token → switch `remote-sync-type` to
`read-write` → add collections. `mbx gitsync` does this in order; if you are
doing it by hand, note that `mb setting set` parses strict JSON, so a string
value needs inner quotes: `mb setting set remote-sync-type '"read-write"'`.

Always read state before mutating:

```bash
docker compose --profile cli run --rm airflow-cli mb git-sync status --json --max-bytes 0
docker compose --profile cli run --rm airflow-cli mb git-sync is-dirty --json
```

For the detail, load the CLI's own skill — it is versioned with the binary and
more current than this file:

```bash
mb skills get git-sync --max-bytes 0
```

## The bug this repo is designed around

**Metabase v63's serializer corrupts incremental transforms.** On import it
silently drops `template-tags` while still applying the incremental target
config, which leaves the transform broken with
`Incremental transform with a native query requires a table variable` — and the
import task reports **success**. A second defect writes a non-portable
per-instance field id for the checkpoint column, so export → import → export is
not a fixed point.

This repo avoids the whole class of problem by using **plain table transforms
only**, which round-trip correctly. That is a hard rule in `CLAUDE.md`, not a
preference. If anyone creates an incremental transform through the UI, the next
sync will quietly corrupt it.

The only detection is a round-trip check: export, import onto a branch, export
again, and diff. Worth doing once after enabling sync, and again after any
Metabase upgrade.

## Two more things worth knowing

**An empty `git-sync dirty` does not mean "nothing is tracked".** Table and
field metadata only serializes for Library-published tables whose Library
collection itself has remote sync enabled. The classic trap is publishing
tables, writing metadata, and getting an empty dirty list — the scope is wrong,
not the metadata.

**Do cleanup deletions before the first export.** Recreating a
same-named object after an export mints a `_2`-suffixed file in the sync repo
that never goes away on its own.
