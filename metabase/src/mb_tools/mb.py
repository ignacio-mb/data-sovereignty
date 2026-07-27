"""Thin wrapper around the `mb` CLI.

Everything that talks to Metabase goes through here rather than through raw HTTP:
`mb` already owns credential resolution, retries, redaction and — importantly —
a versioned, self-describing contract (`mb <cmd> --help --json`). Re-implementing
any of that against the REST API means owning it forever.

House conventions enforced in one place:
  --json --max-bytes 0   full machine-readable output, never truncated
  bodies via --file      multi-line SQL stays readable in the Metabase editor
  --profile after verbs  `mb -p x auth status` silently reports the wrong profile
"""

import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 900


class MbError(RuntimeError):
    """An `mb` invocation failed. Exit code 2 means config/capability, 1 means the
    operation itself failed — see the CLI's documented exit codes."""

    def __init__(self, command, returncode, stdout, stderr):
        self.command = command
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        detail = (stderr or stdout or "").strip()
        super().__init__(f"`{' '.join(command)}` exited {returncode}: {detail}")


def _profile_args():
    profile = os.environ.get("MB_PROFILE", "").strip()
    return ["--profile", profile] if profile else []


def require_cli():
    if shutil.which("mb") is None:
        raise MbError(["mb"], 127, "", "the Metabase CLI is not installed (npm i -g @metabase/cli)")


def run(args, body=None, parse=True, timeout=DEFAULT_TIMEOUT, check=True):
    """Run `mb <args>`. Returns parsed JSON when parse=True, else raw stdout.

    `body` is written to a temp file and passed with --file; embedding multi-line
    SQL on the command line mangles it in the stored representation.
    """
    require_cli()
    command = ["mb", *args]
    with tempfile.TemporaryDirectory(prefix="mbx-") as scratch:
        if body is not None:
            body_path = Path(scratch) / "body.json"
            body_path.write_text(json.dumps(body, indent=2), encoding="utf-8")
            command += ["--file", str(body_path)]

        if parse:
            command += ["--json", "--max-bytes", "0"]
        command += _profile_args()

        log.debug("mb: %s", " ".join(command))
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=False
        )
        if check and completed.returncode != 0:
            raise MbError(command, completed.returncode, completed.stdout, completed.stderr)
        if not parse:
            return completed.stdout
        if not completed.stdout.strip():
            return None
        try:
            return json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise MbError(command, completed.returncode, completed.stdout,
                          f"expected JSON, got: {completed.stdout[:200]}") from exc


def items(payload):
    """Unwrap mb's list envelope {returned, total, limit, truncated, data}."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    return payload.get("data", [])


def help_json(*verbs):
    """The self-description for a command: args, capabilities, input/output schema.

    Used to discover request shapes at runtime instead of hardcoding a body that
    a future Metabase release might change.
    """
    return run([*verbs, "--help", "--json"])
