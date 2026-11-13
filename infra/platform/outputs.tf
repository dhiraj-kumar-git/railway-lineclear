output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "cluster_certificate_authority_data" {
  value = module.eks.cluster_certificate_authority_data
}

output "vpc_id" {
  value = module.vpc.vpc_id
}

output "ecr_repository_urls" {
  value = { for name, repo in aws_ecr_repository.service : name => repo.repository_url }
}

output "rds_address" {
  value = aws_db_instance.main.address
}


output "kafka_bootstrap_brokers_tls" {
  value = try(aws_msk_cluster.main[0].bootstrap_brokers_tls, null)
}

output "train_positions_table" {
  value = aws_dynamodb_table.train_positions_latest.name
}

output "election_lease_table" {
  value = aws_dynamodb_table.election_lease.name
}

output "passenger_notification_url" {
  value = aws_lambda_function_url.passenger_notification.function_url
}

output "notification_queue_url" {
  value = aws_sqs_queue.passenger_notifications.url
}

output "notification_dead_letter_queue_url" {
  value = aws_sqs_queue.passenger_notifications_dead_letter.url
}

output "bulletins_bucket" {
  value = aws_s3_bucket.bulletins.bucket
}

output "analytics_bucket" {
  value = aws_s3_bucket.analytics.id
}

output "emr_cluster_id" {
  value = try(aws_emr_cluster.analytics[0].id, null) # null while enable_emr is false
}

output "loki_internal_address" {
  value = try(aws_lb.loki[0].dns_name, null)
}
