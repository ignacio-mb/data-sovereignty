"""Loading a connector's `extension.py` from beside its spec.

The escape hatch used to live inside this package, at
`ingest_runtime/sources/<name>.py`, three directories from the spec that named
it and inside the very package whose docstring promises that nothing about a
particular API is compiled into the runtime. It was reviewed separately from its
spec, and the only thing linking the two was a string.

Now it sits in the connector's own directory and is loaded by path. Nothing is
installed, nothing is registered, and the spec says only `extensions: true` —
there is no module name left to drift from the file it names.

Loading by path has two consequences worth stating. Extensions cannot use
relative imports into the runtime, which is what forces the public surface in
`extension_api` — a documented seam instead of reaching into private helpers.
And modules get a synthetic package name (`ds_source_ext.<source>`), registered
in sys.modules so tracebacks, pickling and `logging.getLogger(__name__)` all
behave.
"""

from __future__ import annotations

import importlib.util
import sys

# Loaded modules, keyed by resolved path. A run builds auth, resources and
# paginators from the same extension; re-executing the module for each would
# reset whatever run-scoped state it keeps (a worklist cache, a minted token).
_LOADED = {}

PACKAGE = "ds_source_ext"


def load(spec):
    """The module holding what the spec could not express, or None.

    Every connector wants to believe it is ordinary. Pylon is the case that
    proves otherwise: it returns pages claiming `has_next_page: true` while
    carrying no data, and its messages have no cross-issue endpoint, so the
    worklist is a warehouse query rather than an API cursor. Neither is
    expressible as configuration, and pretending otherwise would mean either a
    config language that grows a branch per API, or a connector that quietly
    loses rows.

    So the escape hatch is explicit, named in the spec, and colocated with it. A
    source with none of this has no Python at all.
    """
    if not spec.uses_extension:
        return None

    path = spec.extension_path.resolve()
    cached = _LOADED.get(str(path))
    if cached is not None:
        return cached

    if not path.is_file():
        raise RuntimeError(
            f"{spec.name} declares `extensions: true` but {path} does not exist. "
            f"Either write it or drop the key from the spec."
        )

    module_name = f"{PACKAGE}.{spec.name}"
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(f"{spec.name}: cannot load {path} as a Python module")
    module = importlib.util.module_from_spec(module_spec)
    # Registered before execution so a module importing itself (or anything that
    # inspects sys.modules during import) sees a consistent picture.
    sys.modules[module_name] = module
    try:
        module_spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    _LOADED[str(path)] = module
    return module


def builder_for(spec, resource, extension):
    """The function an extension supplies for one delegated resource.

    `build_<resource>` first, then `build_resource` — a connector whose twelve
    endpoints differ only in data the spec already carries writes one function,
    and one that genuinely needs twelve writes twelve.
    """
    if extension is None:
        return None
    return (getattr(extension, f"build_{resource.name}", None)
            or getattr(extension, "build_resource", None))


def reset(spec=None):
    """Drop cached modules and ask them to clear run-scoped state.

    Extensions may define `reset()` for the caches they keep between resources
    within a run — a worklist read once and reused twelve times, say. Tests need
    that boundary; so does anything that loads two specs in one process.
    """
    targets = [spec] if spec is not None else []
    if not targets:
        modules = list(_LOADED.values())
        keys = list(_LOADED)
    else:
        keys = [str(s.extension_path.resolve()) for s in targets]
        modules = [_LOADED[key] for key in keys if key in _LOADED]
    for module in modules:
        hook = getattr(module, "reset", None)
        if callable(hook):
            hook()
    for key in keys:
        module = _LOADED.pop(key, None)
        if module is not None:
            sys.modules.pop(getattr(module, "__name__", ""), None)
