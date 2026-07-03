terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# Latest Amazon Linux 2023 (arm64, matches Graviton instance types above).
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-arm64"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ---------------------------------------------------------------------------
# IAM: gateways read S3 via an instance role (no static keys on disk).
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "gateway" {
  name               = "nginx-s3-gateway"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "s3_read" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.s3_bucket_name}/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.s3_bucket_name}"]
  }
}

resource "aws_iam_role_policy" "s3_read" {
  name   = "s3-read-only"
  role   = aws_iam_role.gateway.id
  policy = data.aws_iam_policy_document.s3_read.json
}

resource "aws_iam_instance_profile" "gateway" {
  name = "nginx-s3-gateway"
  role = aws_iam_role.gateway.name
}

# ---------------------------------------------------------------------------
# Security groups
# ---------------------------------------------------------------------------
resource "aws_security_group" "lb" {
  name        = "s3cache-lb"
  description = "Load balancer for the S3 cache gateways"
  vpc_id      = var.vpc_id

  ingress {
    description = "Client access to the cache LB"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.allowed_client_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "gateway" {
  name        = "s3cache-gateway"
  description = "nginx-s3-gateway cache servers"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Only the LB may reach the gateways"
    from_port       = 80
    to_port         = 80
    protocol        = "tcp"
    security_groups = [aws_security_group.lb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------------------------------------------------------------------------
# Cache / gateway servers
# ---------------------------------------------------------------------------
resource "aws_instance" "gateway" {
  count                       = var.gateway_count
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type_gateway
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.gateway.id]
  iam_instance_profile        = aws_iam_instance_profile.gateway.name
  key_name                    = var.ssh_key_name
  associate_public_ip_address = false

  user_data = templatefile("${path.module}/gateway_user_data.sh.tftpl", {
    s3_bucket        = var.s3_bucket_name
    s3_region        = var.region
    s3_server        = var.s3_server
    cache_max_size   = var.cache_max_size
    cache_slice_size = var.cache_slice_size
    gateway_image    = var.gateway_image
  })

  tags = { Name = "s3cache-gateway-${count.index}" }
}

# ---------------------------------------------------------------------------
# Consistent-hash load balancer
# ---------------------------------------------------------------------------
resource "aws_instance" "lb" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type_lb
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.lb.id]
  key_name                    = var.ssh_key_name
  associate_public_ip_address = false

  user_data = templatefile("${path.module}/lb_user_data.sh.tftpl", {
    backends = aws_instance.gateway[*].private_ip
  })

  tags = { Name = "s3cache-lb" }
}

output "load_balancer_private_ip" {
  description = "Point DuckDB's s3_endpoint at this host"
  value       = aws_instance.lb.private_ip
}

output "gateway_private_ips" {
  value = aws_instance.gateway[*].private_ip
}
