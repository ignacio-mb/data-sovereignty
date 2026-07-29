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

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  az = var.availability_zone != "" ? var.availability_zone : data.aws_availability_zones.available.names[0]
}

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = var.project }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = { Name = var.project }
}

resource "aws_subnet" "main" {
  vpc_id = aws_vpc.main.id
  # Half the VPC, not all of it: taking the whole range would mean recreating
  # the VPC to add a second subnet later.
  cidr_block              = cidrsubnet(var.vpc_cidr, 1, 0)
  availability_zone       = local.az
  map_public_ip_on_launch = true

  tags = { Name = var.project }
}

resource "aws_route_table" "main" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = var.project }
}

resource "aws_route_table_association" "main" {
  subnet_id      = aws_subnet.main.id
  route_table_id = aws_route_table.main.id
}

resource "aws_security_group" "instance" {
  name        = "${var.project}-instance"
  description = "Egress only. No ingress rules — access is through SSM."
  vpc_id      = aws_vpc.main.id

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
