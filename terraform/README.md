# Terraform Deployment (`terraform/`)

Provisions the AWS EC2 host that runs this pipeline. For architecture, DAGs, and pipeline internals, see the [root DETAILED_README.md](../DETAILED_README.md#terraform-infrastructure-deployment).

## Prerequisites

- Terraform >= 1.5 (`terraform version`)
- AWS CLI, configured with `aws configure` (needs Access Key ID, Secret Access Key, and a default region, e.g. `eu-central-1`)
- An existing local SSH key pair: you can change this keys according your requirement
  ```
  ~/.ssh/smart-city-key
  ~/.ssh/smart-city-key.pub
  ```
  Only the public key is uploaded to AWS (via `aws_key_pair`); the private key never leaves your machine.

## Configure Variables

```bash
cp terraform.tfvars.example terraform.tfvars
```

Set at minimum:

```hcl
aws_region     = "eu-central-1"
instance_type  = "t3.small"
repo_url       = "https://github.com/Birhanegeb/smart-city-traffic-etl.git"
```

Do not commit `terraform.tfvars` if it contains secrets.

## Deploy

```bash
terraform init      # download AWS provider plugins
terraform fmt        # format files
terraform validate   # sanity-check config
terraform plan       # preview: aws_key_pair, aws_security_group, aws_instance
terraform apply      # confirm with "yes" to create the EC2 instance
```

`scripts/user_data.sh` runs automatically on first boot and handles: system update, Docker + Docker Compose + Git install, repository clone, `.env` setup, and `docker compose up -d`.

## After Apply

Terraform prints the instance's public IP/DNS:

```
ec2_public_ip  = "63.178.240.111"
ec2_public_dns = "ec2-63-178-240-111.eu-central-1.compute.amazonaws.com"
```

Connect and verify:

```bash
chmod 400 ~/.ssh/smart-city-key
ssh -i ~/.ssh/smart-city-key ubuntu@<EC2_PUBLIC_IP>

cd ~/smart-city-traffic-etl
docker ps   # airflow-webserver, airflow-scheduler, postgres, spark-master, spark-worker, superset — all "Up"
```

Then open in a browser:
- Airflow: `http://<EC2_PUBLIC_IP>:8080`
- Superset: `http://<EC2_PUBLIC_IP>:8088`

(Credentials come from `.env` — same `AIRFLOW_ADMIN_*` / `SUPERSET_ADMIN_*` variables used in local Docker Compose deployment.)

## Troubleshooting

| Issue | Fix |
| --- | --- |
| `Permissions 0644 for key are too open` | `chmod 400 ~/.ssh/smart-city-key` |
| Lost track of IP/DNS | `terraform output` |
| Container issue on EC2 | `docker compose logs` or `docker logs <container>` |
| Services misbehaving | `docker compose restart` |

## Tear Down

```bash
terraform destroy
```

Removes the EC2 instance, security group, and AWS key pair.