#!/bin/bash

set -e

# Update system
apt update -y
apt upgrade -y

# Install required packages
apt install -y \
    ca-certificates \
    curl \
    gnupg \
    git

# Add Docker official GPG key
install -m 0755 -d /etc/apt/keyrings

curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    -o /etc/apt/keyrings/docker.asc

chmod a+r /etc/apt/keyrings/docker.asc

# Add Docker repository
echo \
"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu \
$(. /etc/os-release && echo $VERSION_CODENAME) stable" \
| tee /etc/apt/sources.list.d/docker.list > /dev/null


# Install Docker Engine + Compose plugin
apt update -y

apt install -y \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin


# Enable Docker
systemctl enable docker
systemctl start docker


# Allow ubuntu user to run Docker without sudo
usermod -aG docker ubuntu


# Clone project
cd /home/ubuntu

git clone https://github.com/Birhanegeb/smart-city-traffic-etl.git
chown -R ubuntu:ubuntu /home/ubuntu/smart-city-traffic-etl

# Start ETL stack
cd smart-city-traffic-etl

docker compose up -d