# A dedicated VPC with one public subnet and a security group that has no
# ingress rules at all.
#
# The instance needs outbound HTTPS — the Pylon API, GitHub, Docker Hub, npm,
# PyPI, and Metabase's licence check — and needs nothing inbound. Security
# groups are stateful, so "no ingress rules" means replies to the instance's
# own connections still arrive while nothing on the internet can open one.
# That is the same inbound posture as a private subnet behind a NAT gateway,
# without the NAT gateway's ~$32/month.
#
# The alternative worth naming: a private subnet with SSM VPC endpoints and no
# NAT. It gives shells but no general egress, so the pipeline cannot reach the
# Pylon API. It does not fit this stack.

# Either shape works. On a shared account that denies ec2:CreateVpc — a common
# guardrail — set existing_subnet_id and everything below except the security
# group is skipped. The inbound posture is identical: the security group is
# created either way, and it has no ingress rules either way.

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_subnet" "existing" {
  count = var.existing_subnet_id != "" ? 1 : 0
  id    = var.existing_subnet_id
}

locals {
  create_network = var.existing_subnet_id == ""

  # The AZ and the VPC follow the subnet when one is given: the data volume is
  # AZ-bound and has to land where the instance does.
  az = local.create_network ? (
    var.availability_zone != "" ? var.availability_zone : data.aws_availability_zones.available.names[0]
  ) : data.aws_subnet.existing[0].availability_zone

  vpc_id    = local.create_network ? aws_vpc.main[0].id : data.aws_subnet.existing[0].vpc_id
  subnet_id = local.create_network ? aws_subnet.main[0].id : var.existing_subnet_id
}

resource "aws_vpc" "main" {
  count                = local.create_network ? 1 : 0
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = var.project }
}

resource "aws_internet_gateway" "main" {
  count  = local.create_network ? 1 : 0
  vpc_id = aws_vpc.main[0].id

  tags = { Name = var.project }
}

resource "aws_subnet" "main" {
  count  = local.create_network ? 1 : 0
  vpc_id = aws_vpc.main[0].id
  # Half the VPC, not all of it: taking the whole range would mean recreating
  # the VPC to add a second subnet later.
  cidr_block              = cidrsubnet(var.vpc_cidr, 1, 0)
  availability_zone       = local.az
  map_public_ip_on_launch = true

  tags = { Name = var.project }
}

resource "aws_route_table" "main" {
  count  = local.create_network ? 1 : 0
  vpc_id = aws_vpc.main[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main[0].id
  }

  tags = { Name = var.project }
}

resource "aws_route_table_association" "main" {
  count          = local.create_network ? 1 : 0
  subnet_id      = aws_subnet.main[0].id
  route_table_id = aws_route_table.main[0].id
}

resource "aws_security_group" "instance" {
  name        = "${var.project}-instance"
  description = "Egress only. No ingress rules — access is through SSM."
  vpc_id      = local.vpc_id

  tags = { Name = var.project }
}

# Deliberately no aws_vpc_security_group_ingress_rule anywhere in this module.
# Metabase, Airflow, the data docs and ClickHouse are reached through SSM port
# forwarding; the containers also bind to 127.0.0.1 on the host, so a future
# ingress rule added by mistake still would not expose them.
resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.instance.id
  description       = "Pylon API, GitHub, container registries, PyPI, npm, AWS APIs"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}
