# Find latest Ubuntu 24.04 AMI
data "aws_ami" "ubuntu" {

  most_recent = true

  owners = [
    "099720109477"
  ]

  filter {
    name = "name"

    values = [
      "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"
    ]
  }
}


# Create SSH key pair in AWS
resource "aws_key_pair" "traffic_key" {

  key_name = "smart-city-traffic-key"

  public_key = file("~/.ssh/smart-city-key.pub")
}


# Create EC2 instance
resource "aws_instance" "traffic_pipeline" {

  ami = data.aws_ami.ubuntu.id

  instance_type = var.instance_type

  key_name = aws_key_pair.traffic_key.key_name

  vpc_security_group_ids = [
    aws_security_group.traffic_sg.id
  ]

  user_data = file("../scripts/user_data.sh")


  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }


  tags = {
    Name = "smart-city-traffic-etl"
  }
}