# Operations Analytics: one private bucket for the Flink job's files, its checkpoints and the
# EMR logs. The job's SQL and scripts are uploaded from services/ops-analytics/flink, so a
# change there reaches EMR with terraform apply.
locals {
  ops_analytics_flink = "${path.module}/../../services/ops-analytics/flink"
}

resource "aws_s3_bucket" "analytics" {
  bucket        = "${var.project}-analytics-${data.aws_caller_identity.current.account_id}"
  force_destroy = true # holds only job files, checkpoints and logs; the rebuild test deletes it
}

resource "aws_s3_bucket_public_access_block" "analytics" {
  bucket                  = aws_s3_bucket.analytics.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "analytics" {
  bucket = aws_s3_bucket.analytics.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Old checkpoints and EMR logs are deleted after a week
resource "aws_s3_bucket_lifecycle_configuration" "analytics" {
  bucket = aws_s3_bucket.analytics.id

  rule {
    id     = "expire-checkpoints"
    status = "Enabled"

    filter {
      prefix = "checkpoints/"
    }

    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-emr-logs"
    status = "Enabled"

    filter {
      prefix = "emr-logs/"
    }

    expiration {
      days = 7
    }
  }
}

resource "aws_s3_object" "ops_analytics_flink" {
  for_each = fileset(local.ops_analytics_flink, "**/*.{sql,sh}")
  bucket   = aws_s3_bucket.analytics.id
  key      = "flink/${each.value}"
  source   = "${local.ops_analytics_flink}/${each.value}"
  etag     = filemd5("${local.ops_analytics_flink}/${each.value}") # a changed file is uploaded again
}
