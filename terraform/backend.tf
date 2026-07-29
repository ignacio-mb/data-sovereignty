# State outlives the laptop. The bucket is created once, out of band — see
# terraform/README.md — rather than by a second root module that would need
# its own state to exist first.
#
# Fill these in and run `terraform init`. Everything else in this directory
# works without editing.
terraform {
  backend "s3" {
    bucket = "REPLACE-ME-data-sovereignty-tfstate"
    key    = "data-sovereignty/prod.tfstate"
    region = "us-east-1"

    encrypt = true
    # Native S3 locking. No DynamoDB table.
    use_lockfile = true
  }
}
