# ---------------------------------------------------------------------------
# policy.tf - what a student's AWS identity is allowed to do
#
# Principle: allow only the AWS services and actions that ../terraform/aws
# uses, only in one region, and block the things that cost money or escalate
# privileges. Reading a policy tells you what the lab really touches in AWS.
#
# Each statement below says which part of the lab needs it.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "student" {

  # Network, jump host, PrivateLink endpoints and the endpoint service
  # (network.tf, jump.tf, egress.tf, ingress.tf, database.tf).
  # Describe/Get calls cannot be limited to particular resources in IAM, so they
  # use "*"; they only read.
  statement {
    sid = "Ec2ReadOnly"
    actions = [
      "ec2:Describe*",
      "ec2:Get*",
    ]
    resources = ["*"]
  }

  statement {
    sid = "Ec2Network"
    actions = [
      "ec2:CreateVpc", "ec2:DeleteVpc", "ec2:ModifyVpcAttribute",
      "ec2:CreateSubnet", "ec2:DeleteSubnet", "ec2:ModifySubnetAttribute",
      "ec2:CreateInternetGateway", "ec2:DeleteInternetGateway",
      "ec2:AttachInternetGateway", "ec2:DetachInternetGateway",
      "ec2:CreateRouteTable", "ec2:DeleteRouteTable",
      "ec2:CreateRoute", "ec2:DeleteRoute",
      "ec2:AssociateRouteTable", "ec2:DisassociateRouteTable",
      "ec2:CreateSecurityGroup", "ec2:DeleteSecurityGroup",
      "ec2:AuthorizeSecurityGroupIngress", "ec2:AuthorizeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupIngress", "ec2:RevokeSecurityGroupEgress",
      "ec2:ModifySecurityGroupRules", "ec2:UpdateSecurityGroupRuleDescriptionsIngress",
      "ec2:CreateTags", "ec2:DeleteTags",
    ]
    resources = ["*"]
  }

  statement {
    sid = "Ec2PrivateLink"
    actions = [
      "ec2:CreateVpcEndpoint", "ec2:DeleteVpcEndpoints", "ec2:ModifyVpcEndpoint",
      "ec2:CreateVpcEndpointServiceConfiguration",
      "ec2:DeleteVpcEndpointServiceConfigurations",
      "ec2:ModifyVpcEndpointServiceConfiguration",
      "ec2:ModifyVpcEndpointServicePermissions",
      "ec2:RejectVpcEndpointConnections",
    ]
    resources = ["*"]
  }

  statement {
    sid = "Ec2JumpHost"
    actions = [
      "ec2:RunInstances", "ec2:TerminateInstances", "ec2:ModifyInstanceAttribute",
      "ec2:ImportKeyPair", "ec2:CreateKeyPair", "ec2:DeleteKeyPair",
    ]
    resources = ["*"]
  }

  # The jump host's operating-system image: terraform looks up the current
  # Amazon Linux 2023 image id through this public parameter (jump.tf).
  statement {
    sid       = "AmiLookup"
    actions   = ["ssm:GetParameter"]
    resources = ["arn:aws:ssm:${var.region}::parameter/aws/service/ami-amazon-linux-latest/*"]
  }

  # The private PostgreSQL database (database.tf).
  statement {
    sid = "Rds"
    actions = [
      "rds:CreateDBInstance", "rds:DeleteDBInstance", "rds:ModifyDBInstance",
      "rds:CreateDBSubnetGroup", "rds:DeleteDBSubnetGroup", "rds:ModifyDBSubnetGroup",
      "rds:AddTagsToResource", "rds:RemoveTagsFromResource", "rds:ListTagsForResource",
      "rds:Describe*",
    ]
    resources = ["*"]
  }

  # The Network Load Balancer in front of the database (egress.tf).
  statement {
    sid = "LoadBalancer"
    actions = [
      "elasticloadbalancing:CreateLoadBalancer", "elasticloadbalancing:DeleteLoadBalancer",
      "elasticloadbalancing:ModifyLoadBalancerAttributes",
      "elasticloadbalancing:SetSubnets", "elasticloadbalancing:SetSecurityGroups",
      "elasticloadbalancing:CreateTargetGroup", "elasticloadbalancing:DeleteTargetGroup",
      "elasticloadbalancing:ModifyTargetGroup", "elasticloadbalancing:ModifyTargetGroupAttributes",
      "elasticloadbalancing:RegisterTargets", "elasticloadbalancing:DeregisterTargets",
      "elasticloadbalancing:CreateListener", "elasticloadbalancing:DeleteListener",
      "elasticloadbalancing:ModifyListener",
      "elasticloadbalancing:AddTags", "elasticloadbalancing:RemoveTags",
      "elasticloadbalancing:Describe*",
    ]
    resources = ["*"]
  }

  # The private DNS zone for the Confluent cluster (ingress.tf). Route 53 is a
  # global service, so it is not limited by region.
  statement {
    sid = "Route53PrivateZone"
    actions = [
      "route53:CreateHostedZone", "route53:DeleteHostedZone",
      "route53:GetHostedZone", "route53:ListHostedZones", "route53:ListHostedZonesByName",
      "route53:ListHostedZonesByVPC",
      "route53:ChangeResourceRecordSets", "route53:ListResourceRecordSets",
      "route53:GetChange", "route53:ChangeTagsForResource", "route53:ListTagsForResource",
      "route53:AssociateVPCWithHostedZone", "route53:DisassociateVPCFromHostedZone",
    ]
    resources = ["*"]
  }

  # AWS creates "service-linked roles" the first time load balancers or RDS are
  # used in an account. Allow only those two, nothing else in IAM.
  statement {
    sid       = "ServiceLinkedRoles"
    actions   = ["iam:CreateServiceLinkedRole"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "iam:AWSServiceName"
      values   = ["elasticloadbalancing.amazonaws.com", "rds.amazonaws.com"]
    }
  }

  # --- Guardrails: explicit denies always win over the allows above ----------

  # Only one region. Global services (IAM, Route 53, STS) are exempt because
  # they have no region.
  statement {
    sid         = "OnlyThisRegion"
    effect      = "Deny"
    not_actions = ["iam:*", "route53:*", "sts:*"]
    resources   = ["*"]
    condition {
      test     = "StringNotEquals"
      variable = "aws:RequestedRegion"
      values   = [var.region]
    }
  }

  # Cost guardrails: one small instance type and one small database class.
  statement {
    sid       = "OnlySmallInstances"
    effect    = "Deny"
    actions   = ["ec2:RunInstances"]
    resources = ["arn:aws:ec2:*:*:instance/*"]
    condition {
      test     = "StringNotEquals"
      variable = "ec2:InstanceType"
      values   = [var.instance_type]
    }
  }

  statement {
    sid       = "OnlySmallDatabases"
    effect    = "Deny"
    actions   = ["rds:CreateDBInstance", "rds:ModifyDBInstance"]
    resources = ["*"]
    condition {
      test     = "StringNotEquals"
      variable = "rds:DatabaseClass"
      values   = [var.db_instance_class]
    }
  }
}

resource "aws_iam_policy" "student" {
  name        = "lab7-student"
  description = "Lets a Lab 7 student run ../terraform/aws in one region"
  policy      = data.aws_iam_policy_document.student.json
}
