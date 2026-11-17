# Lab 7 Terraform: private database + Confluent Cloud over PrivateLink

The lab is built by two Terraform configurations that each need only their own
credentials:

| Directory | Credentials | Builds |
| --- | --- | --- |
| `confluent/` | Confluent Cloud API key | Environment, egress and ingress gateways, Enterprise Kafka cluster, service accounts, Kafka API keys, and Confluent's two endpoints |
| `aws/` | AWS access key (the limited student user from `../admin-iam`) | VPC, private RDS PostgreSQL, jump host, Network Load Balancer, VPC endpoint service, ingress VPC endpoint, Route 53 private zone |

Terraform hides the details that the lab teaches, so every `.tf` file starts with
a comment that says what it creates and which manual command it replaces. Splitting
the configuration also makes the connection between the two clouds visible: three
values cross over by hand. The lab document
(`../LAB-7-JDBC-CONNECTOR-PRIVATE-LINK.md`) explains the architecture and the
student steps that follow.

## The order, and what crosses between the halves

```
1. confluent/  apply   creates the gateways and the cluster
                          |  egress_principal_arn, ingress_service_name
                          v
2. aws/        apply   builds everything in AWS, using those two values
                          |  egress_endpoint_service_name, ingress_vpc_endpoint_id
                          v
3. confluent/  apply   creates Confluent's egress and ingress endpoints
```

Why two applies of `confluent/`? Each PrivateLink connection has two ends, and
each end needs a name that the other cloud creates:

```
egress gateway --principal_arn--> VPC endpoint service --service_name--> Confluent egress endpoint
   (confluent/)                        (aws/egress.tf)                    (confluent/access_points.tf)

ingress gateway --service name--> AWS VPC endpoint --id--> Confluent ingress endpoint
   (confluent/)                    (aws/ingress.tf)         (confluent/access_points.tf)
                                          |
                                          +--dns name--> Route 53 wildcard record (aws/ingress.tf)

database --private IP--> NLB target --> NLB --> VPC endpoint service   (all in aws/)
```

With a single configuration Terraform passes these names itself; here you pass
them, which shows what each side needs from the other. The `confluent/` endpoint
variables are empty on the first apply, so the two endpoints are skipped
(`count = 0`) until step 3. Destroy in reverse: `confluent/` first, then `aws/`.

## Files, in reading order

| File | Contents |
| --- | --- |
| `confluent/versions.tf` | Provider: `confluent` |
| `confluent/variables.tf` | Region, students, and the two AWS values (empty at first) |
| `confluent/confluent.tf` | Environment, gateways, cluster, identities, API keys, connector cleanup on destroy |
| `confluent/access_points.tf` | Confluent egress and ingress endpoints (second apply) |
| `confluent/outputs.tf` | Values printed after apply, including the two for `aws/` |
| `aws/versions.tf` | Providers: `aws`, `random`, `tls`, `local` |
| `aws/variables.tf` | Region, zones, name prefix, your IP, students, and the two Confluent values |
| `aws/network.tf` | VPC, subnets, internet gateway, routes |
| `aws/database.tf` | Private RDS, its firewall, generated passwords, private-IP lookup |
| `aws/jump.tf` | SSH key, jump host, first-boot seeding (`jump-init.sh.tftpl`) |
| `aws/egress.tf` | NLB, endpoint service, disconnect waiter |
| `aws/ingress.tf` | Ingress VPC endpoint, private DNS |
| `aws/outputs.tf` | Values printed after apply, including the two for `confluent/` |

## Before you run

1. AWS credentials that work (`aws sts get-caller-identity`). For a class, use the
   limited user from `../admin-iam`.
2. A Confluent **Cloud** API key in your environment (it never goes in a file):

   ```bash
   confluent api-key create --resource cloud
   export CONFLUENT_CLOUD_API_KEY=<key>
   export CONFLUENT_CLOUD_API_SECRET=<secret>
   ```

3. `curl`, `python3`, and the AWS CLI on this machine (the destroy-time helpers
   use them).
4. The region (default `ap-south-1`) enabled in your AWS account, and the three
   Availability Zone IDs in `aws/variables.tf` (`az_ids`) valid for it. Use the
   same region in both directories.

## Run it

```bash
# 1. Confluent: gateways and cluster (about 2 minutes)
cd confluent
echo 'students = ["s01", "s02", "s03", "s04", "s05", "s06", "s07", "s08", "s09", "s10", "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21", "s22", "s23", "s24", "s25", "s26"]' > terraform.tfvars
terraform init
terraform apply

# 2. AWS: everything else (about 6 minutes)
cd ../aws
cat > terraform.tfvars <<EOF
admin_cidr                     = "$(curl -s https://checkip.amazonaws.com)/32"
students                       = ["s01", "s02", "s03", "s04", "s05", "s06", "s07", "s08", "s09", "s10", "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21", "s22", "s23", "s24", "s25", "s26"]
confluent_egress_principal_arn = "$(terraform -chdir=../confluent output -raw egress_principal_arn)"
confluent_ingress_service_name = "$(terraform -chdir=../confluent output -raw ingress_service_name)"
EOF
terraform init
terraform apply

# 3. Confluent: the two endpoints (about 3 minutes)
cd ../confluent
cat >> terraform.tfvars <<EOF
egress_endpoint_service_name = "$(terraform -chdir=../aws output -raw egress_endpoint_service_name)"
ingress_vpc_endpoint_id      = "$(terraform -chdir=../aws output -raw ingress_vpc_endpoint_id)"
EOF
terraform apply
```

Reading a plan: `+` means create, `~` update in place, `-` destroy, `-/+`
replace. Values marked `(known after apply)` are produced by the cloud provider
(IDs, addresses, secrets). The plans create about 12, 29, and 2 resources.

Then print what you need:

```bash
terraform -chdir=confluent output
terraform -chdir=confluent output -json student_kafka_api_keys
terraform -chdir=confluent output -json admin_kafka_api_key
terraform -chdir=aws output
terraform -chdir=aws output -json student_db_passwords
```

## Things that surprise people

- **State.** Each directory records what it created in its own `terraform.tfstate`.
  It contains secrets (passwords, API secrets, the SSH key), so it is git-ignored
  and must not be shared. Delete it only after `terraform destroy`. If you lose
  it, Terraform no longer knows about the resources and you must remove them by
  hand.
- **Two credentials for the same cluster.** The Cloud API key (environment
  variables) manages the cluster. The Kafka keys in `confluent/` outputs are what
  clients use to read and write data, and they have no permissions until you
  create ACLs (lab step 4).
- **Private cluster.** The cluster's REST API is reachable only inside the VPC.
  That is why the API keys set `disable_wait_for_ready = true`, and why topics and
  ACLs are created from the jump host rather than by Terraform.
- **Names must be unique per account and region.** If several students build their
  own `aws/` stack in one AWS account, give each a different `name_prefix` (for
  example `lab7-s07`), because the load balancer, database and key-pair names
  collide otherwise. An account allows 5 VPCs per region by default.
- **Destroy-order helpers.** Two `terraform_data` resources do their work only on
  `destroy`: one (`confluent/`) deletes any connectors left on the cluster
  (Confluent will not delete a cluster that has them), the other (`aws/`) waits
  for AWS to drop Confluent's connection before deleting the endpoint service.
  You will see their messages during destroy.
- **Cost.** The Enterprise cluster dominates (roughly several dollars per hour).
  Destroy as soon as you finish.

## Destroy

```bash
cd confluent && terraform destroy     # about 2-3 minutes
cd ../aws    && terraform destroy     # about 3-4 minutes
```

Always `confluent/` first. Variables come from each `terraform.tfvars`. If a
destroy stops with an error, run it again; it resumes. Afterwards delete the
Cloud API key you created: `confluent api-key delete <key>`.
