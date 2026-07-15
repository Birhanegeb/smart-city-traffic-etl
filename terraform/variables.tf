variable "aws_region" {
  description = "AWS deployment region"
  default     = "eu-central-1"
}


variable "instance_type" {
  description = "EC2 instance size"
  default     = "t3.small"
}


variable "public_key_path" {
  default = "~/.ssh/smart-city-key.pub"
}


variable "repo_url" {
  description = "GitHub repository"
  default     = "https://github.com/Birhanegeb/smart-city-traffic-etl.git"
}