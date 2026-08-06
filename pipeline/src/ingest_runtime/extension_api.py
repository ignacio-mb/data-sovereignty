"""What a connector's `extension.py` is allowed to use.

An extension lives beside its spec, outside this package, and is loaded by path.
It therefore cannot reach in with a relative import — which is the point. This
module is the contract surface: everything here is supported and will keep
working; everything not here is an internal detail of the runtime and may move.

The seam this replaces was `from ..runtime import _auth, column_hints, ...` — a
private name, imported across a package boundary, by the one file least able to
absorb a rename.

See `sources/CONTRACT.md` for what an extension must return and the obligations
that come with it.
"""

from __future__ import annotations

from .auth import build as auth_for
from .runtime import column_hints, endpoint_params, make_transformer, paced_session, session_for
from .warehouse import warehouse_rows

__all__ = [
    "auth_for",
    "column_hints",
    "endpoint_params",
    "make_transformer",
    "paced_session",
    "session_for",
    "warehouse_rows",
]
