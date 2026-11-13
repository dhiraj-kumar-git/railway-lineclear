# Operations Analytics on EMR: one primary and one core node running Flink on YARN. Everything
# here exists only while enable_emr is true, so the cluster costs nothing on the other days.
locals {
  # EMR's managed IAM policy works only with resources carrying this tag
  emr_managed_policy_tag = { "for-use-with-amazon-emr-managed-policies" = "true" }
}

# A small subnet of its own, so the tag above is not needed on the subnets the VPC module manages
resource "aws_subnet" "emr" {
  count             = var.enable_emr ? 1 : 0
  vpc_id            = module.vpc.vpc_id
  cidr_block        = "10.20.66.0/24"
  availability_zone = module.vpc.azs[0]
  tags              = merge(local.emr_managed_policy_tag, { Name = "${var.project}-emr" })
}

# Private like the EKS subnets: out through the NAT gateway, for S3 and the Fluent Bit packages
resource "aws_route_table_association" "emr" {
  count          = var.enable_emr ? 1 : 0
  subnet_id      = aws_subnet.emr[0].id
  route_table_id = module.vpc.private_route_table_ids[0]
}

# EMR adds the rules it needs between these groups itself; revoke_rules_on_delete lets them be deleted
resource "aws_security_group" "emr_primary" {
  count                  = var.enable_emr ? 1 : 0
  name                   = "${var.project}-emr-primary"
  description            = "EMR primary node, managed by EMR"
  vpc_id                 = module.vpc.vpc_id
  revoke_rules_on_delete = true
  tags                   = local.emr_managed_policy_tag
}

resource "aws_security_group" "emr_core" {
  count                  = var.enable_emr ? 1 : 0
  name                   = "${var.project}-emr-core"
  description            = "EMR core nodes, managed by EMR"
  vpc_id                 = module.vpc.vpc_id
  revoke_rules_on_delete = true
  tags                   = local.emr_managed_policy_tag
}

resource "aws_security_group" "emr_service_access" {
  count                  = var.enable_emr ? 1 : 0
  name                   = "${var.project}-emr-service-access"
  description            = "EMR service access to nodes in a private subnet"
  vpc_id                 = module.vpc.vpc_id
  revoke_rules_on_delete = true
  tags                   = local.emr_managed_policy_tag
}

resource "aws_vpc_security_group_egress_rule" "emr_primary_out" {
  count             = var.enable_emr ? 1 : 0
  security_group_id = aws_security_group.emr_primary[0].id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "S3, Kafka, Loki and package downloads"
}

resource "aws_vpc_security_group_egress_rule" "emr_core_out" {
  count             = var.enable_emr ? 1 : 0
  security_group_id = aws_security_group.emr_core[0].id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "S3, Kafka, Loki and package downloads"
}

# EMR reaches nodes in a private subnet through the service access group: in on 9443 from the
# primary node, out on 8443 to both kinds of node. EMR checks these before it starts the cluster
resource "aws_vpc_security_group_ingress_rule" "emr_service_access_from_primary" {
  count                        = var.enable_emr ? 1 : 0
  security_group_id            = aws_security_group.emr_service_access[0].id
  referenced_security_group_id = aws_security_group.emr_primary[0].id
  ip_protocol                  = "tcp"
  from_port                    = 9443
  to_port                      = 9443
  description                  = "EMR service access from the primary node"
}

resource "aws_vpc_security_group_egress_rule" "emr_service_access_to_primary" {
  count                        = var.enable_emr ? 1 : 0
  security_group_id            = aws_security_group.emr_service_access[0].id
  referenced_security_group_id = aws_security_group.emr_primary[0].id
  ip_protocol                  = "tcp"
  from_port                    = 8443
  to_port                      = 8443
  description                  = "EMR service access to the primary node"
}

resource "aws_vpc_security_group_egress_rule" "emr_service_access_to_core" {
  count                        = var.enable_emr ? 1 : 0
  security_group_id            = aws_security_group.emr_service_access[0].id
  referenced_security_group_id = aws_security_group.emr_core[0].id
  ip_protocol                  = "tcp"
  from_port                    = 8443
  to_port                      = 8443
  description                  = "EMR service access to the core nodes"
}

# Flink runs on both kinds of node, so both may reach the Kafka brokers over TLS
resource "aws_vpc_security_group_ingress_rule" "kafka_from_emr_primary" {
  count                        = var.enable_emr && var.enable_msk ? 1 : 0
  security_group_id            = aws_security_group.kafka[0].id
  referenced_security_group_id = aws_security_group.emr_primary[0].id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
  description                  = "Kafka TLS from the EMR primary node"
}

resource "aws_vpc_security_group_ingress_rule" "kafka_from_emr_core" {
  count                        = var.enable_emr && var.enable_msk ? 1 : 0
  security_group_id            = aws_security_group.kafka[0].id
  referenced_security_group_id = aws_security_group.emr_core[0].id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
  description                  = "Kafka TLS from the EMR core nodes"
}

# The role EMR itself uses to create and run the cluster
resource "aws_iam_role" "emr_service" {
  count = var.enable_emr ? 1 : 0
  name  = "${var.project}-emr-service"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  count      = var.enable_emr ? 1 : 0
  role       = aws_iam_role.emr_service[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}

# The managed policy only passes EMR's default node role, so passing this cluster's is added here
resource "aws_iam_role_policy" "emr_service_pass_node_role" {
  count = var.enable_emr ? 1 : 0
  name  = "pass-node-role"
  role  = aws_iam_role.emr_service[0].id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [{
      Effect    = "Allow"
      Action    = "iam:PassRole"
      Resource  = aws_iam_role.emr_node[0].arn
      Condition = { StringEquals = { "iam:PassedToService" = "ec2.amazonaws.com" } }
    }]
  })
}

# The role of the EMR nodes: the analytics bucket, the Kafka address secret, and reading its own cluster
resource "aws_iam_role" "emr_node" {
  count = var.enable_emr ? 1 : 0
  name  = "${var.project}-emr-node"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "emr_node" {
  count = var.enable_emr ? 1 : 0
  name  = "ops-analytics"
  role  = aws_iam_role.emr_node[0].id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid      = "AnalyticsBucket"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.analytics.arn, "${aws_s3_bucket.analytics.arn}/*"]
      },
      {
        Sid      = "EmrStepTools"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["arn:aws:s3:::${var.region}.elasticmapreduce/*"]
      },
      {
        Sid      = "KafkaAddress"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = ["arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:${var.project}/kafka/connection-*"]
      },
      {
        Sid      = "OwnCluster"
        Effect   = "Allow"
        Action   = ["elasticmapreduce:Describe*", "elasticmapreduce:List*", "ec2:Describe*", "cloudwatch:PutMetricData"]
        Resource = ["*"]
      },
    ]
  })
}

resource "aws_iam_instance_profile" "emr_node" {
  count = var.enable_emr ? 1 : 0
  name  = "${var.project}-emr-node"
  role  = aws_iam_role.emr_node[0].name
}

resource "aws_emr_cluster" "analytics" {
  count                             = var.enable_emr ? 1 : 0
  name                              = "${var.project}-analytics"
  release_label                     = "emr-7.14.0" # Flink 1.20
  applications                      = ["Flink"]
  service_role                      = aws_iam_role.emr_service[0].arn
  log_uri                           = "s3://${aws_s3_bucket.analytics.id}/emr-logs/"
  keep_job_flow_alive_when_no_steps = true # stays up between steps; switched off with enable_emr = false
  termination_protection            = false
  ec2_attributes {
    subnet_id                         = aws_subnet.emr[0].id
    instance_profile                  = aws_iam_instance_profile.emr_node[0].arn
    emr_managed_master_security_group = aws_security_group.emr_primary[0].id
    emr_managed_slave_security_group  = aws_security_group.emr_core[0].id
    service_access_security_group     = aws_security_group.emr_service_access[0].id
  }

  master_instance_group {
    instance_type = var.emr_instance_type
  }

  core_instance_group {
    instance_type  = var.emr_instance_type
    instance_count = 1

    ebs_config {
      size                 = 32
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  bootstrap_action {
    name = "Send Flink logs to Loki"
    path = "s3://${aws_s3_bucket.analytics.id}/${aws_s3_object.ops_analytics_flink["ship-logs-to-loki.sh"].key}"
    args = [aws_lb.loki[0].dns_name, "3100"]
  }

  configurations_json = jsonencode([
    {
      Classification = "flink-conf"

      Properties = {
        "kafka.enabled"                         = "true" # adds the Kafka SQL connector that ships with EMR
        "state.checkpoints.dir"                 = "s3://${aws_s3_bucket.analytics.id}/checkpoints"
        "restart-strategy.type"                 = "fixed-delay"
        "restart-strategy.fixed-delay.attempts" = "20"
        "restart-strategy.fixed-delay.delay"    = "10 s"
      }
    }
  ])

  tags = local.emr_managed_policy_tag

  depends_on = [
    aws_iam_role_policy_attachment.emr_service,
    aws_iam_role_policy.emr_service_pass_node_role,
    aws_iam_role_policy.emr_node,
    aws_route_table_association.emr,
    aws_vpc_security_group_egress_rule.emr_primary_out,
    aws_vpc_security_group_egress_rule.emr_core_out,
    aws_vpc_security_group_ingress_rule.emr_service_access_from_primary,
    aws_vpc_security_group_egress_rule.emr_service_access_to_primary,
    aws_vpc_security_group_egress_rule.emr_service_access_to_core,
  ]
}
