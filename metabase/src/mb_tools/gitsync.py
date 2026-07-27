"""Version Metabase content in a git repository via Enterprise remote sync.

Deferred by design: with no remote configured this is a clean no-op, and nothing
else in the stack depends on it. Set MB_GIT_SYNC_URL and MB_GIT_SYNC_PAT to turn
it on.

The transform SQL in this repo is the *source*; the sync repo holds Metabase's
*serialized state* (cards, dashboards, metrics, metadata). They are
complementary, which is why the sync target is a separate repository — the
serializer owns its whole working tree and its automated commits should not
interleave with code history.
"""

import logging
import os

from . import mb
from .audit import ensure_transforms_collection

log = logging.getLogger(__name__)

# Enterprise installs start read-only, and add-collection is rejected until this
# is flipped. `setting set` parses strict JSON, hence the inner quotes.
READ_WRITE = '"read-write"'


class GitSyncError(RuntimeError):
    pass


def configuration():
    url = os.environ.get("MB_GIT_SYNC_URL", "").strip()
    pat = os.environ.get("MB_GIT_SYNC_PAT", "").strip()
    branch = os.environ.get("MB_GIT_SYNC_BRANCH", "main").strip() or "main"
    return {"url": url, "pat": pat, "branch": branch, "configured": bool(url and pat)}


def _set(key, json_value):
    mb.run(["setting", "set", key, json_value], parse=False)


def configure(config):
    """Point the instance at the remote and switch it to read-write.

    Order matters: the URL, branch and token must be in place before the type
    can flip, and the type must be read-write before collections can be added.
    """
    log.info("configuring remote sync -> %s (%s)", config["url"], config["branch"])
    _set("remote-sync-url", f'"{config["url"]}"')
    _set("remote-sync-branch", f'"{config["branch"]}"')
    _set("remote-sync-token", f'"{config["pat"]}"')
    _set("remote-sync-type", READ_WRITE)


def tracked_collections():
    """Collections whose content should be versioned.

    The transforms collection is the one that matters — it holds everything this
    repo builds. Analytics collections and the Library can be added once they
    exist; adding a collection cascades to its descendants.
    """
    collections = []
    try:
        collections.append(ensure_transforms_collection())
    except mb.MbError as exc:
        raise GitSyncError(f"could not resolve the transforms collection: {exc}") from exc
    return [c for c in collections if c and c.get("id")]


def add_collections(collections):
    for collection in collections:
        log.info("tracking collection %r (id %s)", collection.get("name"), collection["id"])
        mb.run(["git-sync", "add-collection", str(collection["id"])], parse=False, check=False)


def status():
    return mb.run(["git-sync", "status"])


def is_dirty():
    payload = mb.run(["git-sync", "is-dirty"])
    if isinstance(payload, dict):
        return bool(payload.get("dirty", payload.get("is_dirty", False)))
    return bool(payload)


def export(message=None):
    args = ["git-sync", "export", "--wait"]
    if message:
        args += ["--message", message]
    return mb.run(args)


def run(export_now=True):
    """Configure if needed, then export. Returns a short human-readable summary."""
    config = configuration()
    if not config["configured"]:
        return (
            "git-sync is not configured — nothing to do.\n"
            "To enable it, create a dedicated repository (suggested name: "
            "data-sovereignty-metabase-content), then set MB_GIT_SYNC_URL and "
            "MB_GIT_SYNC_PAT in .env and run this again."
        )

    configure(config)
    add_collections(tracked_collections())

    # Always read state before mutating: an export over a dirty remote is how
    # you lose someone else's changes.
    if not is_dirty():
        return "remote sync is configured and up to date — nothing to export."

    if not export_now:
        return "remote sync is configured; content has local changes (export skipped)."

    export(message="Automated export from the data-sovereignty stack")
    return "exported Metabase content to the sync repository."
