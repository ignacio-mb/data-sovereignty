# Running this on AWS

One EC2 instance runs the compose stack. Nothing is exposed to the internet:
the security group has no ingress rules and the containers publish on the
loopback interface, so every UI is reached through an SSM tunnel.

**Merging to `main` is what deploys.** CI gates it, GitHub Actions asks SSM to
run the deploy on the instance, and the check lands on the merge commit. There
is no separate release step and nothing to run by hand.

`terraform/README.md` covers the infrastructure. This is about the stack.

## The shape of it

```
merge to main
   └─ CI: unit, dags, and (only when the image changed) stack-boots on arm64
        └─ deploy job: OIDC → record the commit → SSM
             └─ /usr/local/bin/ds-deploy      pinned on the host: takes the lock,
                │                             checks the commit is an ancestor of
                │                             main, drops to the tree owner
                └─ scripts/stack_update.sh    from the commit being deployed:
                                              defers if work is in flight, builds
                                              before it resets, converges, verifies
```

A timer on the instance also checks every five minutes whether the recorded
commit is the live one. That is what lands a merge that arrived while the
twenty-hour weekly reconcile held the ingest pool, or while the instance was
rebooting.

**The instance's checkout is a deploy artefact, not a workspace.** A deploy
resets it to `origin/main`. Anything uncommitted there is saved to
`/data/deploy/dirty-<sha>.patch` and then overwritten. Edit on a laptop, open a
PR, merge.

## Before the first apply

**Merge this work to `main` first.** The host bootstrap clones `main` and
installs `scripts/ds-deploy.sh` and `scripts/systemd/*` out of the checkout. If
they are not on `main` yet, cloud-init aborts part-way and you get a host with
no SSH key, no systemd units and no CloudWatch agent — while Terraform reports
success.

Deploying from a feature branch instead does not help: `ds-deploy` refuses to
run when the checkout is on any branch other than the one it deploys.

Expect the merge itself to show one failed `Deploy to AWS` check, because the
repository secrets below do not exist yet. That is the correct order — there is
nothing to deploy to until `terraform apply` has run.

## Straight after apply

1. **Confirm the alarm email.** SNS sends a subscription link to the address in
   `terraform.tfvars`; until it is clicked, every alarm fires into a topic with
   no subscriber, and the link expires in two days.

   ```bash
   aws sns list-subscriptions-by-topic --topic-arn "$(cd terraform && terraform output -raw alarm_topic_arn)"
   ```

   A `PendingConfirmation` ARN means it has not been clicked. If the link has
   expired: `terraform apply -replace='aws_sns_topic_subscription.email[0]'`.

2. **Read the boot log.** Terraform cannot see a cloud-init that failed.

   ```bash
   aws ssm start-session --target "$(cd terraform && terraform output -raw instance_id)"
   ```

   Then `sudo tail -40 /var/log/cloud-init-output.log`. The last line must be
   `host preparation complete`. If it is not, fix the cause and re-run the
   script — it is idempotent, and it fast-forwards the checkout on a re-run:

   ```bash
   aws ssm send-command --document-name AWS-RunShellScript --instance-ids "$(cd terraform && terraform output -raw instance_id)" --parameters 'commands=["bash /var/lib/cloud/instance/user-data.txt"]'
   ```

3. **Check the metrics are arriving**, since the two alarms now treat silence
   as a problem and will email you if they are not:

   ```bash
   aws cloudwatch list-metrics --namespace CWAgent --metric-name disk_used_percent
   ```

## First deploy

Assumes `terraform apply` has run and `terraform output instance_id` works.

**1. Point your laptop at the instance.** Take the ssh block from
`terraform output ssh_config` into `~/.ssh/config` — it tunnels ssh over SSM,
which is what makes `rsync` and `ssh -L` work with no inbound port. Then write
`.deploy.env` (git-ignored) at the repo root:

```bash
printf 'DS_REMOTE_HOST=data-sovereignty\nDS_INSTANCE_ID=%s\nAWS_REGION=%s\n' "$(cd terraform && terraform output -raw instance_id)" "$(cd terraform && terraform output -raw region)" > .deploy.env
```

Check it: `ssh data-sovereignty true`.

**2. Push the secrets.** Once, from the laptop, out of your working `.env`:

```bash
make secrets-push
```

Values never touch the terminal. `MB_API_KEY` is deliberately not among them —
it is minted on the instance and only means anything next to the
`metabase-app-data` volume beside it.

**3. Render `.env` on the instance and bring the stack up.**

```bash
make remote CMD='make secrets-pull && make build && make up'
```

`secrets-pull` also sets what is true of that host and not of a laptop: the
container uid, loopback port bindings, and memory limits sized for 16 GB.
`make up` is staged — Metabase first, then `bootstrap_metabase.sh` mints the
API key into `.env`, then the Airflow services are created with it.

The first build takes ten minutes or so: `zstd` has no aarch64 wheel and
compiles from source.

**4. Check it.**

```bash
make remote CMD='make status && make smoke'
```

**5. Look at it.** From the laptop, one command for every UI:

```bash
make tunnels
```

Metabase on :3100, Airflow on :8080, data docs on :8081, ClickHouse on :8124.

## Turning on merge-to-deploy

Three things in GitHub, by hand:

- **Repository secrets** `AWS_DEPLOY_ROLE_ARN` (`terraform output cd_role_arn`),
  `AWS_REGION`, `DEPLOY_INSTANCE_ID`. Secrets rather than variables: none of it
  is sensitive, but this repository is public and a workflow log would
  otherwise publish the account and instance ids.
- **An environment named `production`, created deliberately, with its
  deployment branch restricted to `main`.** The deploy role's trust policy
  matches on `repo:ignacio-mb/data-sovereignty:environment:production`, and the
  subject carries no branch — so the environment's branch policy is what scopes
  deploys, not an approval gate. This matters because a workflow referencing an
  environment that does not exist gets one **auto-created with no protection
  rules at all**. Create it before the first merge.

  The nested policy object needs a JSON body; the `-f 'a[b]=true'` form will
  not build it:

  ```bash
  gh api -X PUT repos/ignacio-mb/data-sovereignty/environments/production --input - <<< '{"deployment_branch_policy":{"protected_branches":true,"custom_branch_policies":false}}'
  ```

- **Add the three secrets only after the first manual deploy has rendered
  `.env`.** Setting them earlier lets a merge record a desired commit for a
  host with no `.env`; the converge timer then fails three times in ten minutes
  and writes `/data/deploy/HOLD`, freezing deploys until someone finds it.
- **Branch protection on `main`**: require a pull request, require the `Lint
  and unit tests` and `DAG integrity` checks, require signed commits, and block
  force pushes. Force pushes matter more than usual here: the deploy decides
  whether to rebuild by diffing against the previously deployed commit, and an
  orphaned commit breaks that diff.

Then prove it, in this order: an empty commit; a comment-only change to a DAG
(should skip the `stack` job and land in about three minutes, without a
rebuild); and a dependency change (should run the arm64 `stack` job and
rebuild).

Worth saying plainly: with one operator and no second reviewer, being able to
merge to `main` means being able to run code on the instance and read every
secret in `.env`. The controls above bound that. They do not remove it.

## Day to day

**Changing SQL, DAGs, suites, or Python.** Merge. Those paths are
bind-mounted and the packages are installed editable, so the change is live
without a rebuild; the dag-processor re-parses on its own.

**Changing dependencies or the image.** Merge. The deploy notices `uv.lock`,
`docker/**` or a `pyproject.toml` moved and rebuilds — in a detached worktree,
*before* it touches the live checkout, so a build that fails leaves the running
stack exactly as it was.

**Bumping the Metabase or Airflow image.** Snapshot first; both run one-way
migrations on start.

```bash
aws ec2 create-snapshot --volume-id "$(cd terraform && terraform output -raw data_volume_id)" --description "pre-upgrade"
```

The image tags live in `docker-compose.yml`, not `.env` — that is deliberate,
so bumping one in git actually reaches the host.

**Stopping deploys while you work on the box:**

```bash
make hold REASON='migrating the warehouse by hand'
```

`make unhold` releases it, and the converge timer lands whatever was waiting.

**What is live right now:**

```bash
make deploy-status
```

**Logs.** `make remote CMD='make logs S=metabase'` for containers; deploy logs
are on the instance at `/data/deploy/run-<sha>.log`. They stay there on purpose
— a public repository's Actions log gets the JSON summary only.

**Disk.** `make disk`. Old build layers are usually what to reclaim first; a
CloudWatch alarm fires at 80% too.

## When a deploy fails

The Actions job tells you which stage and translates the instance's exit code:

| | |
|---|---|
| deferred (76) | Work was in flight. Nothing happened; the converge timer retries within five minutes. |
| refused (77) | `.env` is missing a key compose needs, and re-rendering from Parameter Store did not supply it. |
| rejected (78) | That commit is not an ancestor of `main`. |
| on hold (79) | A `HOLD` file, or the checkout is on some other branch. |
| build failed | The live stack was never touched. Fix and merge again. |
| failed after converge | The box is left exactly as it failed, for you to look at. |

**There is no automatic rollback after converge, deliberately.** `airflow db
migrate` is forward-only, so starting old code against a migrated metadata
database is worse than the failure it would be fixing.

**After three failed attempts at the same commit the converge timer puts
itself on hold** and stops trying — otherwise it would retry every five
minutes forever, rebuilding the image each time, because the diff base only
advances on success. `make unhold` releases it once you have fixed the cause.

If the instance is offline when the merge lands, the job **succeeds with a
warning**: the commit is recorded, and the timer applies it when the instance
returns.

To roll back, run the **Redeploy** workflow with the last commit that worked —
it is in `/data/deploy/history.log` and in `make deploy-status`. The instance
accepts it because it is still an ancestor of `main`.

If GitHub is not an option, from a shell on the instance:

```bash
sudo /usr/local/bin/ds-deploy --sha <good-sha>
```

**This rolls back code, not data.** The warehouse, the dlt cursor and the
Airflow metadata database are forward-only. A missed ingest is picked up by the
next run; a `--mark-deleted` reconcile cannot be un-run.

## Losing the instance

The data volume is the stack: Docker's data-root — every named volume,
including the `warehouse-data` and `dlt-state` pair — plus the checkout and
`.env`. Snapshots are daily, kept a fortnight.

Create a volume from the most recent snapshot, `terraform apply` a replacement
instance against it, and start:

```bash
make remote CMD='sudo systemctl start data-sovereignty'
```

No rebuild: the images are in the data-root too. Half an hour, most of it
waiting for AWS.

**Restore whole volumes, never directories out of them.** `warehouse-data`
holds the rows and `dlt-state` holds the cursor that describes them. They share
one volume precisely so that a single snapshot catches both at the same
instant; restoring one and not the other leaves the pipeline believing it has
already loaded rows that no longer exist.

## Operating it by prompt

The instance has `tmux` and the Claude Code CLI, and the repository's skills
deploy with the code, so the by-prompt workflow works there unchanged:

```bash
ssh -t data-sovereignty tmux new -A -s ops
```

A dropped SSM session then leaves the work running. Remember the checkout is a
deploy target: `make hold` before anything that would be undone by a merge.

## One thing that stays on the laptop

**Schema evolution.** dlt writes the inferred schema to
`/opt/dlt-state/schemas` on the instance rather than into the checkout, so an
ingest cannot dirty the deploy target. When the Pylon API grows a field, the
diff to review comes from a local `ingest run --destination duckdb` run, and
lands through a PR like anything else.
