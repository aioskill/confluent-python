# Lab 7 instructor setup: limited AWS access for students

Run this **once, as an AWS administrator**, before the class. It creates one IAM
user per student who may run `../terraform/aws` and nothing else. Students never
run this directory.

| File | Contents |
| --- | --- |
| `versions.tf` | Providers (`aws`, `local`) |
| `variables.tf` | Student IDs, the one allowed region, the allowed instance sizes |
| `policy.tf` | The permissions, statement by statement, and the explicit denies |
| `users.tf` | The group, the users, their access keys, and the credential files |
| `outputs.tf` | The policy ARN, the users, and where the credential files are |

## What the policy allows

Only what `../terraform/aws` uses: EC2/VPC networking, the jump host and
PrivateLink endpoints; RDS; Network Load Balancers; Route 53 private zones;
the public Amazon Linux image lookup; and creating the two service-linked roles
that load balancers and RDS need. There is no IAM, S3, or any other service.

Three **denies** (they override every allow): any region other than `var.region`;
any EC2 instance type other than `var.instance_type` (`t3.micro`); any RDS class
other than `var.db_instance_class` (`db.t4g.micro`).

What it does **not** do: it cannot stop one student from deleting another
student's lab resources in the same account and region. Use one AWS account per
student if that matters.

## Run it

```bash
terraform init
terraform apply -var 'students=["s01","s02","s03","s04","s05","s06","s07","s08","s09","s10","s11","s12","s13","s14","s15","s16","s17","s18","s19","s20","s21","s22","s23","s24","s25","s26"]'
```

Credential files appear in `student-credentials/<id>.txt` (mode 0600,
git-ignored). Each contains four `aws configure set` commands. Hand the file to
its student privately.

The access keys never expire, and Terraform state holds their secrets (it is
git-ignored; keep it private). When the class ends:

```bash
terraform destroy -var 'students=["s01","s02","s03","s04","s05","s06","s07","s08","s09","s10","s11","s12","s13","s14","s15","s16","s17","s18","s19","s20","s21","s22","s23","s24","s25","s26"]'
```

Use the same list as for `apply`.
