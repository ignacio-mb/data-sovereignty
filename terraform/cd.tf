# Merge-to-deploy: GitHub Actions assumes this role through OIDC — no stored
# AWS keys anywhere — records the target commit in Parameter Store, and asks
# SSM to run the deploy document on the instance. Nothing inbound is opened.

locals {
  account_id = data.aws_caller_identity.current.account_id

  desired_sha_parameter = "${var.ssm_parameter_prefix}/deploy/desired_sha"

  # Kept for the deploy document's description only — the trust policy matches
  # the individual claims instead, because GitHub does not guarantee this
  # string's shape. See the conditions in aws_iam_policy_document.cd_assume.
  #
  # The full subject, not a prefix. `repo:owner/name:*` would match every
  # branch and every pull request in the repository, which is the usual way
  # this pattern is got wrong.
  oidc_subject = "repo:${var.github_repository}:environment:${var.github_environment}"
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.enable_cd && var.create_github_oidc_provider ? 1 : 0

  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_openid_connect_provider" "github" {
  count = var.enable_cd && !var.create_github_oidc_provider ? 1 : 0
  url   = "https://token.actions.githubusercontent.com"
}

locals {
  github_oidc_arn = var.enable_cd ? (
    var.create_github_oidc_provider
    ? aws_iam_openid_connect_provider.github[0].arn
    : data.aws_iam_openid_connect_provider.github[0].arn
  ) : ""
}

data "aws_iam_policy_document" "cd_assume" {
  count = var.enable_cd ? 1 : 0

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Matched claim by claim rather than against the composite `sub`, which is
    # not a stable string. GitHub can issue the subject with the owner and
    # repository ids embedded inline —
    #
    #   repo:owner@132273646/name@1316246347:environment:production
    #
    # rather than the documented `repo:owner/name:environment:production`. A
    # StringEquals on the composite form then never matches, and the only
    # symptom is `AccessDenied ... Not authorized to perform
    # sts:AssumeRoleWithWebIdentity` from a workflow that is configured
    # correctly. Merge-to-deploy never once worked in this repository because of
    # it, and the actual subject is only visible in the CloudTrail event's
    # userIdentity.principalId.
    #
    # These four conditions together are strictly narrower than the subject was:
    # they pin the repository, the environment, and the two ids the subject
    # merely concatenated.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:repository"
      values   = [var.github_repository]
    }

    # Scopes deploys to the environment, which is what the subject was doing.
    # Absent from the token when a job declares no environment, so a workflow
    # that forgets `environment:` is denied rather than quietly trusted.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:environment"
      values   = [var.github_environment]
    }

    # The repository *name* is released for anyone to re-register once it is
    # renamed or transferred. These ids are not.
    dynamic "condition" {
      for_each = var.github_repository_id != "" ? [1] : []
      content {
        test     = "StringEquals"
        variable = "token.actions.githubusercontent.com:repository_id"
        values   = [var.github_repository_id]
      }
    }

    dynamic "condition" {
      for_each = var.github_repository_owner_id != "" ? [1] : []
      content {
        test     = "StringEquals"
        variable = "token.actions.githubusercontent.com:repository_owner_id"
        values   = [var.github_repository_owner_id]
      }
    }
  }
}

resource "aws_iam_role" "cd" {
  count              = var.enable_cd ? 1 : 0
  name               = "${var.project}-github-deploy"
  assume_role_policy = data.aws_iam_policy_document.cd_assume[0].json

  # The arm64 image build can take fifteen minutes; the default hour is
  # comfortably longer than any deploy, and shorter than a stale credential
  # is useful to anyone.
  max_session_duration = 3600
}

data "aws_iam_policy_document" "cd" {
  count = var.enable_cd ? 1 : 0

  # One parameter, not the prefix. CI records which commit *should* be live;
  # it must never be able to write the stack's secrets.
  statement {
    sid       = "RecordDesiredCommit"
    actions   = ["ssm:PutParameter"]
    resources = ["arn:aws:ssm:${var.region}:${local.account_id}:parameter${local.desired_sha_parameter}"]
  }

  # SendCommand authorises against the instance AND the document. Granting
  # only the instance fails with an opaque AccessDenied.
  statement {
    sid     = "RunTheDeployDocument"
    actions = ["ssm:SendCommand"]
    resources = [
      "arn:aws:ec2:${var.region}:${local.account_id}:instance/${aws_instance.main.id}",
      aws_ssm_document.deploy[0].arn,
    ]
  }

  # Both of these are API-level "*" actions — SSM publishes no resource types
  # for them, so they cannot be scoped further. Worth naming the consequence
  # rather than leaving it implied: this role can read the output of any Run
  # Command in the account, not only the deploys it started.
  statement {
    sid       = "PollTheResult"
    actions   = ["ssm:GetCommandInvocation", "ssm:DescribeInstanceInformation"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "cd" {
  count  = var.enable_cd ? 1 : 0
  name   = "${var.project}-github-deploy"
  role   = aws_iam_role.cd[0].id
  policy = data.aws_iam_policy_document.cd[0].json
}

# A document of its own rather than AWS-RunShellScript: the role can then be
# allowed to run exactly this, with a commit-shaped parameter, instead of
# arbitrary shell.
resource "aws_ssm_document" "deploy" {
  count           = var.enable_cd ? 1 : 0
  name            = "${var.project}-deploy"
  document_type   = "Command"
  document_format = "YAML"

  content = yamlencode({
    schemaVersion = "2.2"
    description   = "Deploy a commit of ${var.github_repository} to this instance."
    parameters = {
      Sha = {
        type           = "String"
        description    = "Full 40-character commit sha to deploy."
        allowedPattern = "^[0-9a-f]{40}$"
      }
      AllowInFlight = {
        type          = "String"
        description   = "Deploy even if a DAG run is in flight."
        default       = "false"
        allowedValues = ["true", "false"]
      }
      ForceRebuild = {
        type          = "String"
        description   = "Rebuild the image regardless of which paths changed."
        default       = "false"
        allowedValues = ["true", "false"]
      }
    }
    mainSteps = [{
      action = "aws:runShellScript"
      name   = "deploy"
      inputs = {
        # The wrapper is provisioned on the host, outside the repository, so a
        # commit cannot change the code that decides whether to trust it.
        runCommand = [
          "/usr/local/bin/ds-deploy --sha '{{ Sha }}' --allow-in-flight '{{ AllowInFlight }}' --force-rebuild '{{ ForceRebuild }}'"
        ]
        # The run budget. --timeout-seconds on send-command is only the
        # delivery window and will not stop a build that overruns.
        timeoutSeconds = "5400"
      }
    }]
  })
}
