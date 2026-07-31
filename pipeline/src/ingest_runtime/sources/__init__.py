"""Per-connector escape hatches, one module per source that needs one.

Named by a spec's `extensions:` key and resolved by runtime.extensions(). A
source with none of this has no Python here at all, which is the normal case.
"""
