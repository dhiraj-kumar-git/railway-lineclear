resource "aws_dynamodb_table" "train_positions_latest" {
  name         = "${var.project}-train-positions-latest"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "train_number"

  attribute {
    name = "train_number"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

module "position_ingestion_pod_identity" {
  source  = "terraform-aws-modules/eks-pod-identity/aws"
  version = "~> 2.9"

  name                 = "${var.project}-position-ingestion"
  attach_custom_policy = true
  policy_statements = [
    {
      sid       = "LatestTrainPositions"
      actions   = ["dynamodb:GetItem", "dynamodb:PutItem"]
      resources = [aws_dynamodb_table.train_positions_latest.arn]
    },
  ]

  associations = {
    position_ingestion = {
      cluster_name    = module.eks.cluster_name
      namespace       = "lineclear"
      service_account = "position-ingestion"
    }
  }
}

resource "aws_dynamodb_table" "election_lease" {
  name         = "${var.project}-election-lease"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "election"

  attribute {
    name = "election"
    type = "S"
  }
}

module "leader_election_pod_identity" {
  source  = "terraform-aws-modules/eks-pod-identity/aws"
  version = "~> 2.9"

  name                 = "${var.project}-leader-election"
  attach_custom_policy = true
  policy_statements = [
    {
      sid       = "ElectionLease"
      actions   = ["dynamodb:GetItem", "dynamodb:UpdateItem"]
      resources = [aws_dynamodb_table.election_lease.arn]
    },
  ]

  associations = {
    leader_election = {
      cluster_name    = module.eks.cluster_name
      namespace       = "lineclear"
      service_account = "leader-election"
    }
  }
}