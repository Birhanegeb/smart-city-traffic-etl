# Terraform Deployment Guide - Smart City Traffic ETL Pipeline

## Overview

This directory contains the Terraform Infrastructure-as-Code (IaC) configuration used to automatically deploy the Smart City Traffic ETL Pipeline on AWS EC2.

The purpose of this deployment is to evaluate infrastructure reproducibility by replacing manual server configuration with automated provisioning.

Terraform automatically creates and configures:

* AWS EC2 instance
* Ubuntu 24.04 environment
* SSH key registration
* Security group rules
* Docker installation
* Docker Compose installation
* Application deployment
* ETL pipeline execution environment

---

# Deployment Architecture

```
Local Machine
      |
      | terraform apply
      |
      v
AWS Frankfurt Region
(eu-central-1)
      |
      v
EC2 Ubuntu 24.04 Instance
      |
      +----------------------+
      |                      |
      v                      v
 Docker Engine          Repository Clone
      |                      |
      v                      |
 Docker Compose <-----------+
      |
      +----------------+
      |                |
      v                v
 Apache Airflow      Apache Spark
      |                |
      v                v
 PostgreSQL          Spark Worker
      |
      v
 Apache Superset
```

---

# Requirements

Before deployment, install the following tools on your local machine.

## Terraform

Check installation:

```bash
terraform version
```

Required:

```
Terraform >= 1.5
```

---

## AWS CLI

Check:

```bash
aws --version
```

Configure AWS credentials:

```bash
aws configure
```

Required information:

```
AWS Access Key ID
AWS Secret Access Key
Default Region
Output format
```

Example region:

```
eu-central-1
```

---

## SSH Key

Terraform uses an existing SSH key from your local machine.

Expected files:

```
~/.ssh/

├── smart-city-key
└── smart-city-key.pub
```

The private key stays locally.

The public key is uploaded to AWS using Terraform.

---

# Project Structure

```
smart-city-traffic-etl/

├── terraform/
│
│   ├── main.tf
│   ├── provider.tf
│   ├── versions.tf
│   ├── variables.tf
│   ├── terraform.tfvars
│   ├── terraform.tfvars.example
│   ├── security.tf
│   └── outputs.tf
│
├── scripts/
│
│   └── user_data.sh
│
├── docker-compose.yml
├── Dockerfile
├── dags/
├── spark/
├── superset/
└── utils/
```

---

# Terraform Configuration

## AWS Provider

`provider.tf`

```hcl
provider "aws" {
  region = "eu-central-1"
}
```

The deployment uses AWS Frankfurt.

---

# Variables

`variables.tf`

Example:

```hcl
variable "aws_region" {}

variable "instance_type" {}

variable "repo_url" {}
```

---

# Terraform Variables

Create:

```
terraform.tfvars
```

Example:

```hcl
aws_region = "eu-central-1"

instance_type = "t3.small"

repo_url = "https://github.com/Birhanegeb/smart-city-traffic-etl.git"
```

Do not commit files containing secrets.

---

# SSH Key Pair Creation

Terraform creates the AWS key pair:

```hcl
resource "aws_key_pair" "traffic_key" {

  key_name = "smart-city-traffic-key"

  public_key = file("~/.ssh/smart-city-key.pub")

}
```

Relationship:

```
Local Machine

~/.ssh/smart-city-key.pub

          |
          v

Terraform

          |
          v

AWS EC2 Key Pair

smart-city-traffic-key
```

The private key:

```
~/.ssh/smart-city-key
```

must never be uploaded.

---

# Security Group

The security group opens required application ports.

| Service  | Port |
| -------- | ---- |
| SSH      | 22   |
| Airflow  | 8080 |
| Superset | 8088 |

---

# Automatic Deployment Script

The file:

```
scripts/user_data.sh
```

runs automatically during EC2 creation.

It performs:

1. System update
2. Docker installation
3. Docker Compose installation
4. Git installation
5. Repository cloning
6. Folder permission setup
7. Docker Compose deployment

Execution flow:

```
EC2 Creation

      |

user_data.sh

      |

Install dependencies

      |

Clone repository

      |

Configure environment - cp .env.example .env the edit .env and fill with your values

      |

docker compose up -d
```

---

# Deployment Steps

## 1. Enter Terraform Directory

```bash
cd terraform
```

---

## 2. Initialize Terraform

```bash
terraform init
```

Downloads AWS provider plugins.

---

## 3. Format Configuration

```bash
terraform fmt
```

---

## 4. Validate Configuration

```bash
terraform validate
```

Expected:

```
Success! The configuration is valid.
```

---

## 5. Preview Infrastructure

```bash
terraform plan
```

Expected resources:

```
+ aws_key_pair
+ aws_security_group
+ aws_instance
```

---

## 6. Create AWS Infrastructure

Run:

```bash
terraform apply
```

Confirm:

```
yes
```

Terraform creates the EC2 instance.

---

# Connect to EC2 After Deployment

After successful deployment Terraform displays:

Example:

```
Outputs:

ec2_public_ip =
"63.178.240.111"

ec2_public_dns =
"ec2-63-178-240-111.eu-central-1.compute.amazonaws.com"
```

---

## Verify SSH Key

Check:

```bash
ls ~/.ssh/
```

Expected:

```
smart-city-key
smart-city-key.pub
```

Set permission:

```bash
chmod 400 ~/.ssh/smart-city-key
```

---

## SSH Connection

Use:

```bash
ssh -i ~/.ssh/smart-city-key ubuntu@<EC2_PUBLIC_IP>
```

Example:

```bash
ssh -i ~/.ssh/smart-city-key ubuntu@63.178.240.111
```

Successful login:

```
ubuntu@ip-xxx-xxx-xxx-xxx:~$
```

---

# Verify Deployment on EC2

## Check Docker

```bash
docker --version
```

---

## Check Docker Compose

```bash
docker compose version
```

---

## Navigate to Project

```bash
cd ~/smart-city-traffic-etl
```

---

## Check Containers

```bash
docker ps
```

Expected services:

```
airflow-webserver
airflow-scheduler
postgres
spark-master
spark-worker
superset
```

All containers should show:

```
STATUS: Up
```

---

# Access Applications

## Apache Airflow

Open:

```
http://<EC2_PUBLIC_IP>:8080
```

Example:

```
http://63.178.240.111:8080
```

---

## Apache Superset

Open:

```
http://<EC2_PUBLIC_IP>:8088
```

Example:

```
http://63.178.240.111:8088
```

Credentials are defined in:

```
.env
```

Example:

```env
AIRFLOW_ADMIN_USERNAME=
AIRFLOW_ADMIN_PASSWORD=

SUPERSET_ADMIN_USERNAME=
SUPERSET_ADMIN_PASSWORD=
```

---

# Troubleshooting

## SSH Permission Error

Fix:

```bash
chmod 400 ~/.ssh/smart-city-key
```

---

## Terraform Outputs

View EC2 information:

```bash
terraform output
```

---

## Container Logs

Example:

```bash
docker logs airflow-webserver-container
```

or:

```bash
docker compose logs
```

---

## Restart Services

```bash
docker compose restart
```

---

# Destroy Deployment

To remove AWS resources:

```bash
terraform destroy
```

Confirm:

```
yes
```

This removes:

* EC2 instance
* Security group
* AWS key pair

---

# Reproducibility Evaluation

This Terraform deployment enables comparison between manual and automated deployment.

## Manual Deployment

```
Create EC2 manually

        |

SSH into server

        |

Install Docker

        |

Clone repository

        |

Configure environment

        |

Start containers
```

---

## Terraform Deployment

```
terraform apply

        |

EC2 automatically created

        |

Environment configured

        |

Application deployed

        |

Services available
```

The Terraform approach improves:

* Deployment consistency
* Infrastructure reproducibility
* Environment portability
* Reduction of manual configuration errors

This supports the evaluation of infrastructure automation for scalable ETL pipeline deployment.
