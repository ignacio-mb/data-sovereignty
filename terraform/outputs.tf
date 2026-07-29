output "instance_id" {
  description = "SSM target, and the HostName in the ssh-over-SSM config."
  value       = aws_instance.main.id
}

output "region" {
  value = var.region
}

output "data_volume_id" {
  description = "The volume every snapshot and every restore is about."
  value       = aws_ebs_volume.data.id
}

output "cd_role_arn" {
  description = "Set as the AWS_DEPLOY_ROLE_ARN repository secret in GitHub."
  value       = var.enable_cd ? aws_iam_role.cd[0].arn : ""
}

output "deploy_document_name" {
  value = var.enable_cd ? aws_ssm_document.deploy[0].name : ""
}

output "alarm_topic_arn" {
  value = aws_sns_topic.alarms.arn
}

output "ssh_config" {
  description = "Append to ~/.ssh/config to get ssh, rsync and tunnels over SSM."
  value       = <<-EOT
    Host ${var.project}
      HostName ${aws_instance.main.id}
      User ubuntu
      ProxyCommand sh -c "aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters 'portNumber=%p' --region ${var.region}"
      StrictHostKeyChecking accept-new
  EOT
}

output "tunnels" {
  description = "One command for every UI, once the ssh config above is in place."
  value       = "ssh -N -L 3100:localhost:3100 -L 8080:localhost:8080 -L 8081:localhost:8081 -L 8124:localhost:8124 ${var.project}"
}

output "port_forward_metabase" {
  description = "Reaching a UI without the ssh config. One session per port."
  value       = "aws ssm start-session --target ${aws_instance.main.id} --region ${var.region} --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"3100\"],\"localPortNumber\":[\"3100\"]}'"
}
