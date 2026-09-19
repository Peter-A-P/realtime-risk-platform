# A VPC of its own rather than the account's default one, so that everything
# the stack owns is tagged, created here and destroyed here, and nothing of
# project 04's shares a security boundary with it.

resource "aws_vpc" "main" {
  cidr_block           = "10.90.0.0/24"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "verdict" }
}

# One public subnet in the one zone. Public so the instance reaches ECR, SSM
# and Cloudflare without a NAT gateway, which would cost more per month than
# the instance. Nothing reaches the instance: see the security group.
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.90.0.0/25"
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = true

  tags = { Name = "verdict-public" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = { Name = "verdict" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "verdict-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Outbound only. There is no ingress rule, and a test asserts there never is:
# the dashboard leaves through the Cloudflare Tunnel, which the instance dials
# out to, and a person reaches the instance through SSM Session Manager, which
# it also dials out to. No SSH, no open port, no Elastic IP.
resource "aws_security_group" "instance" {
  name                   = "verdict-instance"
  description            = "verdict live instance: outbound only"
  vpc_id                 = aws_vpc.main.id
  revoke_rules_on_delete = true

  egress {
    description = "ECR, SSM, Cloudflare, package mirrors"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "verdict-instance" }
}
