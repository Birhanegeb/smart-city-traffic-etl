## Terraform Infrastructure Deployment

This contains the Terraform configuration for deploying the Smart City Traffic ETL pipeline to an AWS EC2 instance.

### What the Deployment Does

Running Terraform automatically:

1. Creates an AWS EC2 instance in the Frankfurt region (`eu-central-1`).
2. Uses the latest Ubuntu 24.04 AMI.
3. Creates and attaches the configured SSH key pair.
4. Configures the required security group.
5. Creates a 20 GB GP3 root volume.
6. Runs `scripts/user_data.sh` during EC2 initialization.
7. Installs Docker Engine and Docker Compose.
8. Clones the ETL project from GitHub.
9. Starts the Docker Compose environment.

The `.env` file is **not included in the repository** because it contains passwords, API keys, and other sensitive configuration. It must therefore be configured after the EC2 instance is created.

### EC2 Instance Configuration

The default EC2 instance type is t3.small:

instance_type = "t3.small"

The instance type can be changed according to the computational requirements of the pipeline. The t3.small instance is suitable for deployment verification and checking the Airflow orchestration layer, but its limited CPU and memory resources may not be sufficient to execute the complete ETL pipeline.

For full pipeline execution, a larger EC2 instance is recommended.

The instance type can be changed in terraform/variables.tf:

variable "instance_type" {
  description = "EC2 instance size"
  default     = "t3.small"
}

Alternatively, it can be overridden during deployment:

terraform apply -var="instance_type=t3.medium"

For example:

t3.small   → Suitable for deployment verification
t3.medium  → More suitable for running the complete pipeline
t3.large   → Recommended when additional memory and processing capacity are required

The appropriate instance type depends on the workload and AWS pricing. Larger instances may incur additional AWS costs.
### Project Structure

The `scripts` directory is located one level above the Terraform directory:

```text
smart-city-traffic-etl/
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── scripts/
│   └── user_data.sh
├── docker-compose.yml
├── dags/
├── spark/
└── ...
```

The Terraform configuration references the initialization script using:

```hcl
user_data = file("../scripts/user_data.sh")
```

### Deployment Steps

#### 1. Configure AWS Credentials

Configure the AWS CLI on the local machine:

```bash
aws configure
```

The deployment uses the Frankfurt region:

```text
eu-central-1
```

#### 2. Navigate to the Terraform Directory

```bash
cd terraform
```

#### 3. Initialize Terraform

```bash
terraform init
```

#### 4. Review the Deployment Plan

```bash
terraform plan
```

#### 5. Create the AWS Infrastructure

```bash
terraform apply
```

Enter `yes` when prompted.

After successful deployment, Terraform displays the EC2 public IP and DNS name.

#### 6. Connect to the EC2 Instance

```bash
ssh -i ~/.ssh/smart-city-key ubuntu@<EC2_PUBLIC_IP>
```

During instance initialization, `user_data.sh` automatically installs Docker and Docker Compose, clones the repository, and starts the Docker Compose environment.

#### 7. Configure the `.env` File

The `.env` file must be copied from the local machine because it contains sensitive credentials and API keys.

From the local machine:

```bash
scp -i ~/.ssh/smart-city-key .env ubuntu@<EC2_PUBLIC_IP>:~/smart-city-traffic-etl/.env
```

Then connect to the EC2 instance:

```bash
ssh -i ~/.ssh/smart-city-key ubuntu@<EC2_PUBLIC_IP>
```

Navigate to the project:

```bash
cd ~/smart-city-traffic-etl
```

Verify the file:

```bash
ls -lh .env
```

#### 8. Restart the Docker Environment

Since the initial Docker Compose startup occurs before the `.env` file is copied, restart the services after configuring the environment:

```bash
docker compose down
docker compose up -d
```

Check the services:

```bash
docker compose ps
```

#### 9. Access Airflow

Open the Airflow web interface using the EC2 public IP:

```text
http://<EC2_PUBLIC_IP>:8080
```

The Airflow interface should display the five configured ETL DAGs.

#### 10. Destroy the Deployment

When the cloud deployment is no longer required:

```bash
terraform destroy
```

Confirm with:

```text
yes
```

This removes the Terraform-managed AWS resources.

