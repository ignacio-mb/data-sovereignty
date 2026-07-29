variable "region" {
  description = "AWS region. Must match the region in backend.tf."
  type        = string
  default     = "us-east-1"
}

variable "availability_zone" {
  description = <<-EOT
    AZ for the subnet, the instance and the data volume. EBS is AZ-bound, so
    moving AZs means restoring the data volume from a snapshot — this is the
    single-AZ availability contract, stated in README.md.
    Empty picks the region's first available AZ.
  EOT
  type        = string
  default     = ""
}

variable "project" {
  description = "Name prefix and tag applied to every resource."
  type        = string
  default     = "data-sovereignty"
}

variable "vpc_cidr" {
  description = "CIDR for the dedicated VPC. Nothing else lives in it."
  type        = string
  default     = "10.20.0.0/24"
}

# ─── Instance ────────────────────────────────────────────────────────────────

variable "instance_type" {
  description = <<-EOT
    Graviton, 4 vCPU / 16 GB. The memory budget is tight but closes with the
    Metabase cap in docker-compose.yml: ClickHouse 6g, Metabase ~3g, four
    Airflow services ~3g, two Postgres ~0.5g, ingest bursts ~1.5g, host ~1g.
    Step up to r8g.xlarge (32 GB) when sources are added, not before.

    Stay on arm64. Switching architectures invalidates every image built and
    pulled into /data/docker and forces a full rebuild.
  EOT
  type        = string
  default     = "m8g.xlarge"
}

variable "ami_id" {
  description = "Override the Ubuntu 24.04 arm64 AMI. Empty resolves the current one from SSM."
  type        = string
  default     = ""
}

variable "root_volume_gb" {
  description = "OS disk. Holds the swapfile; all stack data lives on the data volume."
  type        = number
  default     = 20
}

variable "data_volume_gb" {
  description = <<-EOT
    /data — Docker's data-root, the repo checkout and .env. gp3 grows online
    (modify-volume + resize2fs), so start modest.
  EOT
  type        = number
  default     = 100
}

variable "operator_ssh_public_key" {
  description = <<-EOT
    Public key installed for the ubuntu user, reachable only through
    SSH-over-SSM (ProxyCommand AWS-StartSSHSession). There is no inbound SSH
    port and no EC2 key pair. Empty means SSM Session Manager only, which is
    enough for shells but not for rsync or ssh -L.
  EOT
  type        = string
  default     = ""
}

# ─── Access ──────────────────────────────────────────────────────────────────

variable "enable_tailscale" {
  description = "Join the instance to a tailnet with `tailscale up --ssh`."
  type        = bool
  default     = false
}

variable "tailscale_authkey_parameter" {
  description = "SecureString parameter holding a Tailscale auth key. Read at first boot only."
  type        = string
  default     = "/data-sovereignty/prod/TAILSCALE_AUTHKEY"
}

variable "ssm_parameter_prefix" {
  description = <<-EOT
    Parameter Store path the instance may read. The stack's secrets live here
    as individual SecureStrings and are created out of band with
    `aws ssm put-parameter`, never as Terraform resources — Terraform would
    put them in state.
  EOT
  type        = string
  default     = "/data-sovereignty/prod"
}

# ─── Application ─────────────────────────────────────────────────────────────

variable "repo_url" {
  description = "Public HTTPS clone URL. Public means the instance needs no credentials to pull."
  type        = string
  default     = "https://github.com/ignacio-mb/data-sovereignty.git"
}

variable "repo_branch" {
  description = "The branch that is deployed. Only this branch is ever fetched or reset to."
  type        = string
  default     = "main"
}

variable "mb_cli_version" {
  description = <<-EOT
    scripts/bootstrap_metabase.sh runs on the HOST and needs `mb`, so the
    instance installs it globally. Keep this pinned to the same version as
    MB_CLI_SPEC in docker/airflow/Dockerfile or the host and the image drift.
  EOT
  type        = string
  default     = "0.2.2"
}

# ─── Backups and monitoring ──────────────────────────────────────────────────

variable "snapshot_daily_retain" {
  description = "Daily crash-consistent snapshots of the data volume to keep."
  type        = number
  default     = 14
}

variable "snapshot_weekly_retain" {
  description = "Weekly snapshots to keep, on top of the dailies."
  type        = number
  default     = 4
}

variable "alarm_email" {
  description = "Subscribed to the alarm topic. Empty creates the topic with no subscription."
  type        = string
  default     = ""
}

variable "enable_cloudwatch_agent" {
  description = <<-EOT
    Ships disk and memory metrics. Nothing inside the stack watches host disk,
    and a full /data breaks ClickHouse and corrupts in-flight dlt loads.
  EOT
  type        = bool
  default     = true
}

variable "data_disk_alarm_threshold" {
  description = "Percent used on /data that raises the alarm."
  type        = number
  default     = 80
}

# ─── Continuous deployment ───────────────────────────────────────────────────

variable "enable_cd" {
  description = "Create the GitHub OIDC deploy role and the deploy SSM document."
  type        = bool
  default     = true
}

variable "github_repository" {
  description = "owner/name. Used to build the OIDC subject the deploy role trusts."
  type        = string
  default     = "ignacio-mb/data-sovereignty"
}

variable "github_environment" {
  description = <<-EOT
    The GitHub Environment the deploy job declares. The trust policy matches
    the fully-qualified subject `repo:<repo>:environment:<env>`, which is what
    keeps a fork PR — or any other branch — from assuming the role.
  EOT
  type        = string
  default     = "production"
}

variable "github_repository_id" {
  description = <<-EOT
    Numeric repository id, from `gh api repos/<owner>/<name> --jq .id`.
    Pinned alongside the name because a repo *name* is released for anyone to
    re-register after a rename or transfer; the id is not.
    Empty omits the condition — set it.
  EOT
  type        = string
  default     = ""
}

variable "github_repository_owner_id" {
  description = "Numeric owner id, from `gh api users/<owner> --jq .id`. Empty omits the condition."
  type        = string
  default     = ""
}

variable "create_github_oidc_provider" {
  description = <<-EOT
    False if the account already has the token.actions.githubusercontent.com
    provider (an account can only have one). Check with
    `aws iam list-open-id-connect-providers`.
  EOT
  type        = bool
  default     = true
}
