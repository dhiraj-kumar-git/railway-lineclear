# Team access: each member signs in as their own IAM user, and that user can do only one thing:
# assume the member's role. The roles carry the permissions, each limited to what that member's
# tasks need. Inside the cluster, an EKS access entry maps each role to Kubernetes groups whose
# RBAC rules live in gitops/cluster-config, so Argo CD applies them like everything else.
#
# Access keys and console passwords are issued per member with the AWS CLI after terraform apply,
# so no credential is ever stored in the Terraform state.

locals {
  team = {
    dhiraj     = "control-office"
    riteshbabu = "analytics-gitops"
    saunak     = "notifications-storage"
    abhinav    = "observability-scaling"
  }

  # Every member's role is in this group: read-only view of the cluster, port-forward to the
  # dashboards. Each role is also in a group of its own name for anything beyond that.
  team_kubernetes_group = "${var.project}-team"

  # Read-only calls the AWS console makes on the pages of one resource. Granted on that resource
  # only, so a member's console pages load fully without reaching anything else.
  lambda_function_read = [
    "lambda:GetFunction",
    "lambda:GetFunctionConfiguration",
    "lambda:GetFunctionUrlConfig",
    "lambda:GetPolicy",
    "lambda:ListTags",
    "lambda:GetFunctionConcurrency",
    "lambda:GetFunctionCodeSigningConfig",
    "lambda:GetRuntimeManagementConfig",
    "lambda:GetFunctionEventInvokeConfig",
    "lambda:GetFunctionRecursionConfig",
    "lambda:ListAliases",
    "lambda:ListVersionsByFunction",
    "lambda:ListFunctionUrlConfigs",
    "lambda:ListProvisionedConcurrencyConfigs",
    "lambda:ListFunctionEventInvokeConfigs",
  ]
  dynamodb_table_read = [
    "dynamodb:Describe*",
    "dynamodb:ListTagsOfResource",
    "dynamodb:Scan",
    "dynamodb:Query",
    "dynamodb:GetItem",
  ]
  s3_bucket_read = [
    "s3:ListBucket",
    "s3:ListBucketVersions",
    "s3:GetBucket*",
    "s3:GetEncryptionConfiguration",
    "s3:GetLifecycleConfiguration",
    "s3:GetReplicationConfiguration",
    "s3:GetAccelerateConfiguration",
    "s3:GetIntelligentTieringConfiguration",
    "s3:GetInventoryConfiguration",
    "s3:GetAnalyticsConfiguration",
    "s3:GetMetricsConfiguration",
  ]
  s3_object_read = [
    "s3:GetObject",
    "s3:GetObjectAttributes",
    "s3:GetObjectTagging",
    "s3:GetObjectVersion",
  ]
  project_lambda_logs = "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.project}-*"
  log_events_read = [
    "logs:FilterLogEvents",
    "logs:GetLogEvents",
    "logs:DescribeLogStreams",
    "logs:DescribeMetricFilters",
    "logs:DescribeSubscriptionFilters",
    "logs:ListTagsForResource",
    "logs:ListTagsLogGroup",
    "logs:StartQuery",
  ]

  # What each role may do besides the common part below. A role that is not listed for an action
  # is refused it, which the demo shows (for example, only the control office reads the staff key).
  team_role_statements = {
    "control-office" = [
      {
        Sid      = "ReadStaffKey"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = aws_secretsmanager_secret.staff_api_key.arn
      },
      {
        # Lists secret names for the console page; values stay refused except the staff key above
        Sid      = "ListSecretNames"
        Effect   = "Allow"
        Action   = ["secretsmanager:ListSecrets"]
        Resource = "*"
      },
      {
        Sid      = "ReadPassengerFunction"
        Effect   = "Allow"
        Action   = local.lambda_function_read
        Resource = aws_lambda_function.passenger_notification.arn
      },
      {
        Sid      = "ReadAnalyticsResults"
        Effect   = "Allow"
        Action   = local.dynamodb_table_read
        Resource = aws_dynamodb_table.ops_results.arn
      },
      {
        Sid      = "SeeAnalyticsCluster"
        Effect   = "Allow"
        Action   = ["elasticmapreduce:Describe*", "elasticmapreduce:List*"]
        Resource = "*"
      },
    ]

    "analytics-gitops" = [
      {
        Sid    = "SeeAnalyticsCluster"
        Effect = "Allow"
        Action = [
          "elasticmapreduce:Describe*",
          "elasticmapreduce:List*",
          "elasticmapreduce:GetManagedScalingPolicy",
          "elasticmapreduce:GetAutoTerminationPolicy",
          "elasticmapreduce:GetBlockPublicAccessConfiguration",
        ]
        Resource = "*"
      },
      {
        # Start and stop the Flink job steps, only on this project's EMR cluster
        Sid       = "RunFlinkSteps"
        Effect    = "Allow"
        Action    = ["elasticmapreduce:AddJobFlowSteps", "elasticmapreduce:CancelSteps"]
        Resource  = "*"
        Condition = { StringEquals = { "aws:ResourceTag/Project" = var.project } }
      },
      {
        Sid      = "ReadAnalyticsBucket"
        Effect   = "Allow"
        Action   = local.s3_bucket_read
        Resource = aws_s3_bucket.analytics.arn
      },
      {
        Sid      = "ReadAnalyticsFiles"
        Effect   = "Allow"
        Action   = local.s3_object_read
        Resource = "${aws_s3_bucket.analytics.arn}/*"
      },
      {
        Sid      = "ReadAnalyticsResults"
        Effect   = "Allow"
        Action   = local.dynamodb_table_read
        Resource = aws_dynamodb_table.ops_results.arn
      },
      {
        Sid    = "SeeKafka"
        Effect = "Allow"
        Action = [
          "kafka:Describe*",
          "kafka:List*",
          "kafka:GetBootstrapBrokers",
          "kafka:GetCompatibleKafkaVersions",
        ]
        Resource = "*"
      },
    ]

    "notifications-storage" = [
      {
        Sid    = "SeeNotificationQueues"
        Effect = "Allow"
        Action = [
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ListQueueTags",
          "sqs:ListDeadLetterSourceQueues",
          "sqs:ListMessageMoveTasks",
        ]
        Resource = [
          aws_sqs_queue.passenger_notifications.arn,
          aws_sqs_queue.passenger_notifications_dead_letter.arn,
        ]
      },
      {
        Sid      = "ListQueues"
        Effect   = "Allow"
        Action   = ["sqs:ListQueues"]
        Resource = "*"
      },
      {
        Sid      = "SeePassengerFunction"
        Effect   = "Allow"
        Action   = local.lambda_function_read
        Resource = aws_lambda_function.passenger_notification.arn
      },
      {
        Sid      = "SeeQueueTrigger"
        Effect   = "Allow"
        Action   = ["lambda:ListEventSourceMappings", "lambda:GetEventSourceMapping"]
        Resource = "*"
      },
      {
        Sid      = "ReadProjectFunctionLogs"
        Effect   = "Allow"
        Action   = local.log_events_read
        Resource = [local.project_lambda_logs, "${local.project_lambda_logs}:*"]
      },
      {
        Sid      = "ReadBulletinsBucket"
        Effect   = "Allow"
        Action   = local.s3_bucket_read
        Resource = aws_s3_bucket.bulletins.arn
      },
      {
        Sid      = "ReadBulletins"
        Effect   = "Allow"
        Action   = local.s3_object_read
        Resource = "${aws_s3_bucket.bulletins.arn}/*"
      },
      {
        Sid      = "ReadPassengerNotices"
        Effect   = "Allow"
        Action   = local.dynamodb_table_read
        Resource = aws_dynamodb_table.passenger_notices.arn
      },
      {
        # Database details only: the connection passwords are in Secrets Manager, which stays refused
        Sid      = "SeeDatabase"
        Effect   = "Allow"
        Action   = ["rds:Describe*", "rds:ListTagsForResource"]
        Resource = "*"
      },
    ]

    "observability-scaling" = [
      {
        Sid      = "ReadElectionLease"
        Effect   = "Allow"
        Action   = local.dynamodb_table_read
        Resource = aws_dynamodb_table.election_lease.arn
      },
      {
        Sid      = "ReadMetricsAndAlarms"
        Effect   = "Allow"
        Action   = ["cloudwatch:Describe*", "cloudwatch:Get*", "cloudwatch:List*"]
        Resource = "*"
      },
      {
        Sid      = "ReadProjectFunctionLogs"
        Effect   = "Allow"
        Action   = local.log_events_read
        Resource = [local.project_lambda_logs, "${local.project_lambda_logs}:*"]
      },
    ]
  }
}

resource "aws_iam_user" "team" {
  for_each = local.team

  name          = "${var.project}-${each.key}"
  path          = "/${var.project}/"
  force_destroy = true # the teardown also removes the access key and console password issued outside Terraform
}

resource "aws_iam_role" "team" {
  for_each = local.team

  name                 = "${var.project}-${each.value}"
  max_session_duration = 14400 # 4 hours, enough for a rehearsal or the demo without signing in again

  # Only the member's own user may assume the role, and the session must carry that user's name,
  # so CloudTrail shows who did what (for example assumed-role/lineclear-analytics-gitops/lineclear-riteshbabu).
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = aws_iam_user.team[each.key].arn }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:RoleSessionName" = "$${aws:username}" } }
    }]
  })
}

resource "aws_iam_user_policy" "assume_own_role" {
  for_each = local.team

  name = "assume-own-role"
  user = aws_iam_user.team[each.key].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "AssumeOwnRole"
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = aws_iam_role.team[each.key].arn
      },
      {
        # The console asks for a new password at first sign-in
        Sid      = "ChangeOwnPassword"
        Effect   = "Allow"
        Action   = ["iam:ChangePassword", "iam:GetUser"]
        Resource = aws_iam_user.team[each.key].arn
      },
      {
        Sid      = "ReadPasswordPolicy"
        Effect   = "Allow"
        Action   = "iam:GetAccountPasswordPolicy"
        Resource = "*"
      },
    ]
  })
}

# Common to every role: find the cluster, so kubectl can be set up with aws eks update-kubeconfig
resource "aws_iam_role_policy" "team_common" {
  for_each = local.team

  name = "find-cluster"
  role = aws_iam_role.team[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "DescribeCluster"
        Effect   = "Allow"
        Action   = ["eks:DescribeCluster"]
        Resource = module.eks.cluster_arn
      },
      {
        Sid      = "ListClusters"
        Effect   = "Allow"
        Action   = ["eks:ListClusters"]
        Resource = "*"
      },
    ]
  })
}

# Common to every role: the list pages and lookups the AWS console makes on its way to a
# member's own resources. Names and settings only; contents stay limited to each role's resources.
resource "aws_iam_role_policy" "team_console" {
  for_each = local.team

  name = "console-read"
  role = aws_iam_role.team[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ClusterPages"
        Effect   = "Allow"
        Action   = ["eks:Describe*", "eks:List*", "eks:AccessKubernetesApi"]
        Resource = "*"
      },
      {
        Sid    = "ListResourceNames"
        Effect = "Allow"
        Action = [
          "s3:ListAllMyBuckets",
          "s3:GetBucketLocation",
          "dynamodb:ListTables",
          "dynamodb:DescribeLimits",
          "lambda:ListFunctions",
          "logs:DescribeLogGroups",
          "logs:GetQueryResults",
          "logs:DescribeQueries",
        ]
        Resource = "*"
      },
      {
        Sid    = "MetricsAndNetworkDetails"
        Effect = "Allow"
        Action = [
          "cloudwatch:GetMetricData",
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:ListMetrics",
          "cloudwatch:DescribeAlarms",
          "cloudwatch:DescribeAlarmsForMetric",
          "ec2:DescribeVpcs",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeAvailabilityZones",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "team_role" {
  for_each = local.team

  name = each.value
  role = aws_iam_role.team[each.key].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = local.team_role_statements[each.value]
  })
}

resource "aws_eks_access_entry" "team" {
  for_each = local.team

  cluster_name      = module.eks.cluster_name
  principal_arn     = aws_iam_role.team[each.key].arn
  kubernetes_groups = [local.team_kubernetes_group, "${var.project}-${each.value}"]
  type              = "STANDARD"
}

output "team_roles" {
  description = "Each member's IAM user and the role it may assume"
  value = {
    for member, role in local.team : member => {
      user     = aws_iam_user.team[member].name
      role_arn = aws_iam_role.team[member].arn
    }
  }
}
