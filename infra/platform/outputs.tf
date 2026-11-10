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