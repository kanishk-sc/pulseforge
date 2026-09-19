output "load_balancer_url" {
  value = "http://${aws_lb.main.dns_name}"
}

output "ecr_repository_urls" {
  value = { for name, repository in aws_ecr_repository.service : name => repository.repository_url }
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.id
}

output "database_secret_arn" {
  value     = aws_secretsmanager_secret.database.arn
  sensitive = true
}
