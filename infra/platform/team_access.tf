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
        Sid      = "ReadPassengerFunction"
        Effect   = "Allow"
        Action   = ["lambda:GetFunctionConfiguration", "lambda:GetFunctionUrlConfig"]
        Resource = aws_lambda_function.passenger_notification.arn
      },
      {
        Sid      = "ReadAnalyticsResults"
        Effect   = "Allow"
        Action   = ["dynamodb:DescribeTable", "dynamodb:Scan"]
        Resource = aws_dynamodb_table.ops_results.arn
      },
      {
        Sid      = "SeeAnalyticsCluster"
        Effect   = "Allow"
        Action   = ["elasticmapreduce:ListClusters"]
        Resource = "*"
      },
    ]

    "analytics-gitops" = [
      {
        Sid    = "SeeAnalyticsCluster"
        Effect = "Allow"
        Action = [
          "elasticmapreduce:ListClusters",
          "elasticmapreduce:DescribeCluster",
          "elasticmapreduce:ListSteps",
          "elasticmapreduce:DescribeStep",
          "elasticmapreduce:ListInstances",
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
        Sid      = "ListAnalyticsBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.analytics.arn
      },
      {
        Sid      = "ReadAnalyticsBucket"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.analytics.arn}/*"
      },
      {
        Sid      = "ReadAnalyticsResults"
        Effect   = "Allow"
        Action   = ["dynamodb:DescribeTable", "dynamodb:Scan", "dynamodb:Query", "dynamodb:GetItem"]
        Resource = aws_dynamodb_table.ops_results.arn
      },
      {
        Sid      = "SeeKafka"
        Effect   = "Allow"
        Action   = ["kafka:ListClustersV2", "kafka:DescribeClusterV2", "kafka:GetBootstrapBrokers"]
        Resource = "*"
      },
    ]

    "notifications-storage" = [
      {
        Sid    = "SeeNotificationQueues"
        Effect = "Allow"
        Action = ["sqs:GetQueueAttributes", "sqs:GetQueueUrl"]
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
        Sid    = "SeePassengerFunction"
        Effect = "Allow"
        Action = [
          "lambda:GetFunction",
          "lambda:GetFunctionConfiguration",
          "lambda:GetFunctionUrlConfig",
        ]
        Resource = aws_lambda_function.passenger_notification.arn
      },
      {
        Sid      = "SeeQueueTrigger"
        Effect   = "Allow"
        Action   = ["lambda:ListEventSourceMappings"]
        Resource = "*"
      },
      {
        Sid      = "ReadPassengerFunctionLogs"
        Effect   = "Allow"
        Action   = ["logs:FilterLogEvents", "logs:GetLogEvents", "logs:DescribeLogStreams"]
        Resource = "${aws_cloudwatch_log_group.passenger_notification.arn}:*"
      },
      {
        Sid      = "ListLogGroups"
        Effect   = "Allow"
        Action   = ["logs:DescribeLogGroups"]
        Resource = "*"
      },
      {
        Sid      = "ListBulletinsBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.bulletins.arn
      },
      {
        Sid      = "ReadBulletins"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.bulletins.arn}/*"
      },
      {
        Sid      = "ReadPassengerNotices"
        Effect   = "Allow"
        Action   = ["dynamodb:DescribeTable", "dynamodb:Scan", "dynamodb:Query", "dynamodb:GetItem"]
        Resource = aws_dynamodb_table.passenger_notices.arn
      },
      {
        Sid      = "SeeDatabase"
        Effect   = "Allow"
        Action   = ["rds:DescribeDBInstances"]
        Resource = "*"
      },
    ]

    "observability-scaling" = [
      {
        Sid      = "ReadElectionLease"
        Effect   = "Allow"
        Action   = ["dynamodb:DescribeTable", "dynamodb:Scan", "dynamodb:GetItem"]
        Resource = aws_dynamodb_table.election_lease.arn
      },
      {
        Sid    = "ReadMetrics"
        Effect = "Allow"
        Action = [
          "cloudwatch:GetMetricData",
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:ListMetrics",
          "cloudwatch:DescribeAlarms",
        ]
        Resource = "*"
      },
      {
        Sid      = "ListLogGroups"
        Effect   = "Allow"
        Action   = ["logs:DescribeLogGroups"]
        Resource = "*"
      },
      {
        Sid      = "ReadProjectLogs"
        Effect   = "Allow"
        Action   = ["logs:FilterLogEvents", "logs:GetLogEvents", "logs:DescribeLogStreams"]
        Resource = "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.project}-*:*"
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
