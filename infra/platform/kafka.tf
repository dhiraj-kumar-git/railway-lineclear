resource "aws_security_group" "kafka" {
  count = var.enable_msk ? 1 : 0

  name        = "${var.project}-kafka"
  description = "Kafka over TLS from the EKS nodes, and between the brokers"
  vpc_id      = module.vpc.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "kafka_from_nodes" {
  count = var.enable_msk ? 1 : 0

  security_group_id            = aws_security_group.kafka[0].id
  referenced_security_group_id = module.eks.node_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
  description                  = "Kafka TLS from EKS nodes"
}

resource "aws_vpc_security_group_ingress_rule" "kafka_between_brokers" {
  count = var.enable_msk ? 1 : 0

  security_group_id            = aws_security_group.kafka[0].id
  referenced_security_group_id = aws_security_group.kafka[0].id
  ip_protocol                  = "-1"
  description                  = "Brokers replicate to each other"
}

resource "aws_vpc_security_group_egress_rule" "kafka_out" {
  count = var.enable_msk ? 1 : 0

  security_group_id = aws_security_group.kafka[0].id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Brokers reach each other and AWS services"
}

resource "aws_msk_cluster" "main" {
  count = var.enable_msk ? 1 : 0

  cluster_name           = var.project
  kafka_version          = var.kafka_version
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.kafka[0].id]

    storage_info {
      ebs_storage_info {
        volume_size = 10
      }
    }
  }

  client_authentication {
    unauthenticated = true
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
}

resource "aws_secretsmanager_secret" "kafka" {
  count = var.enable_msk ? 1 : 0

  name                    = "${var.project}/kafka/connection"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "kafka" {
  count = var.enable_msk ? 1 : 0

  secret_id = aws_secretsmanager_secret.kafka[0].id
  secret_string = jsonencode({
    bootstrap_servers = aws_msk_cluster.main[0].bootstrap_brokers_tls
    security_protocol = "SSL"
  })
}