# Skills

`data-stack` is the router. It is the only skill worth loading speculatively;
everything else is loaded for one task and then forgotten.

## The contract

**One router, small leaves.** A leaf covers one intent and stays short enough to
read in full. If a leaf grows past roughly 150 lines, the intent was too broad.

**Side effects go through `make` or `airflow-cli`.** Targets carry the right
flags, the right container and the right ordering. A skill that reaches around
them will drift from what actually runs in production.

**Discover Metabase behaviour at runtime, never vendor it.** `mb` ships its own
skills, versioned with the binary:

```bash
mb skills get <name> --max-bytes 0     # the default 24576 cap truncates
mb <command> --help --json             # input/output JSON Schema
```

Copying that content here guarantees it goes stale on the next `mb` upgrade.
Link to it instead.

**Write the traps, not the happy path.** The commands are in `make help`. What a
skill is for is the judgement around them: which mode the user actually wants,
what a failure implies, what looks broken but is not. If a section could be
replaced by `--help` output, cut it.

**Never print a secret.** Check whether a variable is set; never echo its value.

## Adding a skill

Add a row to the router's route table and a directory with `SKILL.md`. The
frontmatter needs `name`, a `description` **ending in real trigger phrases** in
the user's words, and `allowed-tools`. Triggers are how the skill gets found —
"is my pipeline healthy?" is a trigger, "pipeline observability" is not.
