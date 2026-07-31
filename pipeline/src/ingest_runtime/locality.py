"""Is the warehouse this process is about to write to actually the local one?

`localhost` names a port, not a machine. An SSH tunnel to the instance binds
127.0.0.1 while Docker binds 0.0.0.0, so the tunnel wins and a command that
looks entirely local writes to PRODUCTION, with nothing in its output to say so.
That is not hypothetical: `make bootstrap` rotated the instance's Metabase API
key twice that way in one afternoon, reporting ordinary local success both times.

Inside the containers compose injects `warehouse-db`, so a legitimate run never
sees a loopback address and never reaches this. Only host-side invocations do,
and those are the ones the runbooks tell you not to make anyway — an out-of-band
run also races the incremental cursor its Airflow pool exists to serialise.

Lives in `pipeline` because `quality` already depends on it for the spec parser
and the dependency never runs the other way. Both CLIs share this one
implementation deliberately: `ingest` grew the guard first, `dq` did not, and
for a while the twin command in the same virtualenv reading the same env var
would happily CREATE TABLE and INSERT into the instance.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

WAREHOUSE_HOST_ENV = "DESTINATION__CLICKHOUSE__CREDENTIALS__HOST"

# 0.0.0.0 counts: it is not a destination anyone means to write to, and treating
# it as remote-and-fine would be a hole rather than a convenience.
_LOOPBACK = {"localhost", "127.0.0.1", "::1", "0.0.0.0", ""}


class RemoteWarehouseRefused(RuntimeError):
    """Raised instead of writing to a warehouse whose identity is ambiguous."""


def refuse_loopback_warehouse(action, override_env, alternatives):
    """Raise unless the warehouse address is unambiguously not loopback.

    `action` names what was about to happen, `override_env` is the escape hatch
    for that command, and `alternatives` is the text telling the operator how to
    do it properly. The escape hatch warns rather than staying silent: bypassing
    a locality check is exactly the moment you want a line in the log saying so.
    """
    host = os.environ.get(WAREHOUSE_HOST_ENV, "").strip()
    if host.lower() not in _LOOPBACK:
        return

    if os.environ.get(override_env, "").strip():
        log.warning(
            "[guard] %s allowed against loopback warehouse %r by %s — confirm which "
            "instance that address reaches before trusting the result",
            action, host or "(unset)", override_env,
        )
        return

    raise RemoteWarehouseRefused(
        f"refusing {action}: the warehouse host is {host or '(unset)'!r}, which "
        f"names a port rather than a machine.\n\n"
        f"A tunnel to the instance binds loopback and wins over Docker, so this "
        f"could reach PRODUCTION and would look identical either way.\n\n"
        f"{alternatives}\n"
        f"Set {override_env}=1 only once you have confirmed by hand which "
        f"warehouse that address actually reaches."
    )
