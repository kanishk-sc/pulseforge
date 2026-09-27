output "instance_id" {
  description = "Private SSM-managed host; no SSH or public application address is provisioned."
  value       = aws_instance.platform.id
}

output "private_ip" {
  value = aws_instance.platform.private_ip
}

output "data_volume_id" {
  description = "Protected encrypted data volume. Never format or discard without a verified backup."
  value       = aws_ebs_volume.platform_data.id
}

output "backup_bucket_name" {
  value = aws_s3_bucket.backup.id
}

output "session_manager_dashboard_command" {
  description = "Operator-authenticated localhost forward; requires AWS CLI and Session Manager plugin."
  value       = "aws ssm start-session --target ${aws_instance.platform.id} --region ${var.aws_region} --document-name AWS-StartPortForwardingSession --parameters portNumber=15173,localPortNumber=15173"
}
