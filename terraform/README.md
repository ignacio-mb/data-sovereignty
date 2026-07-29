# Infrastructure

One EC2 instance running the compose stack, reachable only through SSM. This
directory builds the *host*; `docs/deploy.md` covers getting the stack onto it
and keeping it there.

## What it creates

| | |
|---|---|
| VPC | `10.20.0.0/24`, one public subnet, internet gateway. Security group with **no ingress rules** — egress only. |
| Instance | `m8g.xlarge` (Graviton, 4 vCPU / 16 GB), Ubuntu 24.04 arm64, IMDSv2 with a one-hop limit so containers cannot reach the instance role. |
| Storage | 20 GB root (OS + swapfile) and a 100 GB gp3 volume at `/data` holding Docker's data-root, the repo and `.env`. |
| Backups | DLM: daily snapshots kept 14 days, weekly kept 4 weeks. |
| Monitoring | System status check with EC2 auto-recover; CloudWatch agent for disk and memory, alarms to SNS. |
| CD | GitHub OIDC role and an SSM document, so a merge to `main` deploys. |

Roughly **$145/month** on demand, about $106 with a one-year savings plan.

## One-time setup

State lives in S3 so it outlives the laptop. Create the bucket once — a second
root module to manage one bucket would need state of its own:

```bash
aws s3api create-bucket --bucket YOUR-BUCKET --region us-east-1
```

```bash
aws s3api put-bucket-versioning --bucket YOUR-BUCKET --versioning-configuration Status=Enabled
```

```bash
aws s3api put-public-access-block --bucket YOUR-BUCKET --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

Then put the bucket name in `backend.tf`, copy `terraform.tfvars.example` to
`terraform.tfvars` (git-ignored — it holds your account's ids), and fill it in.

The two GitHub ids the deploy role pins:

```bash
gh api repos/ignacio-mb/data-sovereignty --jq .id && gh api users/ignacio-mb --jq .id
```

If the account already has a GitHub OIDC provider — an account may only have
one — set `create_github_oidc_provider = false`:

```bash
aws iam list-open-id-connect-providers
```

## Apply

```bash
terraform init && terraform apply
```

Secrets are **not** Terraform resources; Terraform would put them in state.
Push them from the laptop instead, once: `make secrets-push` (see
`docs/deploy.md`).

## Getting in

There is no inbound port and no EC2 key pair. `terraform output ssh_config`
prints a block for `~/.ssh/config` that tunnels ssh — and therefore rsync and
`ssh -L` — over SSM; `terraform output tunnels` prints the one command that
forwards every UI. Without the ssh config, `terraform output
port_forward_metabase` shows the raw one-port-per-session form.

A plain shell needs neither:

```bash
aws ssm start-session --target "$(terraform output -raw instance_id)"
```

## Things worth knowing before you change something

**Editing `user_data.sh.tftpl` does nothing to a running instance.** cloud-init
runs once per instance and `user_data_replace_on_change` is off on purpose, so
an edit does not silently replace the box either. The script is idempotent; to
apply it, run it again through SSM:

```bash
aws ssm send-command --document-name AWS-RunShellScript --instance-ids "$(terraform output -raw instance_id)" --parameters 'commands=["bash /var/lib/cloud/instance/user-data.txt"]'
```

**Restore whole volumes, never directories.** `warehouse-data` holds the rows
and `dlt-state` holds the cursor that describes them. They are both Docker
volumes on the one data volume precisely so a single snapshot captures them at
the same instant. Restoring one out of a snapshot and not the other leaves the
pipeline believing it already loaded rows that no longer exist.

**Resizing**: stop the instance, change `instance_type`, apply, start. `/data`
is untouched. Stay on Graviton — moving to x86 invalidates every image in the
data-root and forces a full rebuild.

**Single AZ, deliberately.** EBS is AZ-bound. Losing the AZ means creating a
volume from the most recent snapshot in another one and applying with a new
`availability_zone`: up to 24 hours of data at risk, an hour or two of work.
That is the availability contract of a single-host stack; anything better is a
different architecture.

**`prevent_destroy` is set on the data volume.** `terraform destroy` will
refuse until you remove it, which is the intent.
