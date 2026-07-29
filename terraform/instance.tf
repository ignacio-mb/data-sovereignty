# Ubuntu over Amazon Linux: the SSM agent is preinstalled, Docker's apt repo
# ships the compose plugin for arm64 (AL2023's does not), and NodeSource and
# Tailscale both publish Ubuntu repos — the host needs Node because
# scripts/bootstrap_metabase.sh runs `mb` outside the containers.
data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id"
}

# The data volume exists independently of the instance: the instance can be
# replaced, resized or re-imaged without it, and it is the unit of backup.
resource "aws_ebs_volume" "data" {
  availability_zone = local.az
  size              = var.data_volume_gb
  type              = "gp3"
  encrypted         = true
  # prevent_destroy below already refuses a destroy. This is for the day
  # someone removes that deliberately: the last thing the volume does on its
  # way out is leave a snapshot behind.
  final_snapshot = true

  tags = {
    Name = "${var.project}-data"
    # The DLM policy selects on this tag.
    Backup = var.project
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_instance" "main" {
  ami                    = var.ami_id != "" ? var.ami_id : data.aws_ssm_parameter.ubuntu.value
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.main.id
  vpc_security_group_ids = [aws_security_group.instance.id]
  iam_instance_profile   = aws_iam_instance_profile.instance.name

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
    # One hop. The host can reach IMDS; containers, which are a hop away over
    # the docker bridge, cannot — so an Airflow UI that grants code execution
    # cannot mint AWS credentials.
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
    tags        = { Name = "${var.project}-root" }
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    data_volume_id  = aws_ebs_volume.data.id
    region          = var.region
    project         = var.project
    repo_url        = var.repo_url
    repo_branch     = var.repo_branch
    mb_cli_version  = var.mb_cli_version
    param_prefix    = var.ssm_parameter_prefix
    ssh_public_key  = var.operator_ssh_public_key
    tailscale       = var.enable_tailscale
    tailscale_param = var.tailscale_authkey_parameter
    cw_agent        = var.enable_cloudwatch_agent
    cw_config_param = local.cw_agent_parameter_name
  })

  # Editing user_data does not re-run cloud-init on a live instance and must
  # not silently replace the box either. Re-apply host changes by running the
  # same script through SSM Run Command; it is written to be idempotent.
  user_data_replace_on_change = false

  tags = { Name = var.project }

  lifecycle {
    ignore_changes = [ami]
  }
}

resource "aws_volume_attachment" "data" {
  # Nitro ignores this name and enumerates the disk as an NVMe device, so
  # user-data finds it by volume id under /dev/disk/by-id instead.
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.data.id
  instance_id = aws_instance.main.id
}
