variable "region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket holding DuckLake parquet data files"
  type        = string
}

variable "s3_server" {
  description = "S3 endpoint host (regional endpoint recommended)"
  type        = string
  default     = "s3.us-east-1.amazonaws.com"
}

variable "gateway_count" {
  description = "Number of cache/gateway servers"
  type        = number
  default     = 2
}

variable "instance_type_gateway" {
  description = "Instance type for cache servers (needs fast local disk / good NIC)"
  type        = string
  default     = "c6gd.xlarge" # Graviton + NVMe instance store for cache
}

variable "instance_type_lb" {
  description = "Instance type for the consistent-hash load balancer"
  type        = string
  default     = "c6g.large"
}

variable "cache_max_size" {
  description = "Max on-disk cache size per gateway (PROXY_CACHE_MAX_SIZE)"
  type        = string
  default     = "100g"
}

variable "cache_slice_size" {
  description = "Byte-range slice size (PROXY_CACHE_SLICE_SIZE). Tune to workload."
  type        = string
  default     = "8m"
}

variable "gateway_image" {
  description = "nginx-s3-gateway container image"
  type        = string
  default     = "ghcr.io/nginxinc/nginx-s3-gateway/nginx-oss-s3-gateway:latest"
}

variable "vpc_id" {
  description = "VPC to deploy into"
  type        = string
}

variable "subnet_id" {
  description = "Subnet (private recommended) to place instances in"
  type        = string
}

variable "allowed_client_cidrs" {
  description = "CIDRs allowed to reach the load balancer (your DuckDB compute)"
  type        = list(string)
  default     = ["10.0.0.0/8"]
}

variable "ssh_key_name" {
  description = "Optional EC2 key pair name for SSH access"
  type        = string
  default     = null
}
