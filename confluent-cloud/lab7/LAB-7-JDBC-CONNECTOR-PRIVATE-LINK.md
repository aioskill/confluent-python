# Lab 7: JDBC Source Connector to a Private AWS Database (PrivateLink)

In this lab a fully managed Confluent Cloud JDBC source connector reads rows
from a PostgreSQL database in Amazon RDS that has **no public address**. The
connection runs over AWS PrivateLink: Confluent Cloud opens an *egress*
endpoint to a VPC endpoint service in your AWS account, so the database
traffic never crosses the public internet.

> **Status:** this lab was run end to end in `ap-south-1` (Mumbai) with an
> Enterprise cluster, an RDS PostgreSQL database, and the managed PostgreSQL
> source connector, and all commands below were fixed to match what worked.

Use a sandbox AWS account and a sandbox Confluent Cloud organization. This lab
creates billable resources (an Enterprise Kafka cluster, an RDS instance, a
Network Load Balancer, two VPC endpoints, and a small EC2 instance). Finish the
Cleanup section. Expect roughly $5 per hour while everything runs, most of it
the Enterprise cluster (it must be highly available, so it has at least two
eCKUs).

## Learning objectives

- Explain why a managed connector needs a private path to a database that is
  not publicly reachable, and compare PrivateLink with the public option.
- Build the AWS side of an egress PrivateLink and an ingress path to the private cluster: a private RDS PostgreSQL
  database, an internal Network Load Balancer, and a VPC endpoint service.
- Create a Confluent Cloud egress gateway and egress endpoint, and approve the
  connection from AWS.
- Deploy the managed PostgreSQL source (JDBC) connector against the private
  endpoint and confirm that rows arrive in a Kafka topic.
- Troubleshoot the most common PrivateLink failures.

## How the pieces fit together

```
Confluent Cloud (environment)                 Your AWS account (VPC)
+--------------------------------+            +----------------------------------+
| Managed JDBC connector         |            |  VPC endpoint service            |
|   -> egress gateway/endpoint --+--PrivateLink-->  -> internal NLB (TCP 5432)    |
| Enterprise Kafka cluster       |            |       -> RDS PostgreSQL (private)|
+--------------------------------+            +----------------------------------+
```

- An **egress gateway** is a Confluent Cloud network object in your
  environment, in one cloud region.
- An **egress endpoint** (an *access point*) on that gateway connects to a
  specific AWS **VPC endpoint service** that you publish.
- The endpoint service must front a **Network Load Balancer**. RDS cannot be
  published directly, so the NLB forwards TCP port 5432 to the database's
  private IP address.
- Private networking for managed connectors requires an **Enterprise** (or
  Dedicated) Kafka cluster. A connector on a Basic or Standard cluster cannot use
  the egress endpoint: its connection to the endpoint's DNS name simply times out.
- An Enterprise cluster is itself reachable **only privately**. Its Kafka and
  REST endpoints (`...aws.private.confluent.cloud`) do not resolve from the
  internet, so you also need an *ingress* PrivateLink path from your VPC to
  create topics and read data. Steps 2–3 build it, and you work from the jump host.

## Prerequisites

- Terraform 1.5 or later (`install_terraform_ubuntu.sh` in the parent directory).
- The AWS CLI, configured with the limited student credentials from your
  instructor (see "Install and configure the AWS CLI" below). Terraform also uses
  it while destroying.
- `curl` and `python3` on the machine that runs Terraform (used while destroying).
- The Confluent CLI, logged in (`confluent login --save`), only to create the
  Cloud API key below and to manage connectors.
- A Confluent Cloud account that can create environments, gateways, an
  Enterprise Kafka cluster, service accounts, and connectors.

### Install and configure the AWS CLI

Install AWS CLI v2 (Linux x86_64; use `aarch64` in the URL on ARM machines):

```bash
sudo apt-get update && sudo apt-get install -y unzip curl
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
unzip -q -o /tmp/awscliv2.zip -d /tmp
sudo /tmp/aws/install --update
aws --version
```

Configure your credentials (the access key ID, secret access key, and a
session token if one is issued):

```bash
aws configure set aws_access_key_id <AWS_ACCESS_KEY_ID>
aws configure set aws_secret_access_key <AWS_SECRET_ACCESS_KEY>
aws configure set aws_session_token <AWS_SESSION_TOKEN>    # only for temporary credentials
aws configure set region <AWS_REGION>
aws sts get-caller-identity
```

`aws sts get-caller-identity` must print an account and ARN. Temporary tokens
expire (commonly after 1 to 12 hours); if later commands fail with
`ExpiredToken`, get new values and repeat the `aws configure set` commands.

> **Note:** The training instructor will provide the AWS access key, secret key,
> session token (if any), and the region to use. Normally you receive a small
> file that contains these `aws configure set` commands with your values filled
> in; just run them. Do not use personal or
> production AWS credentials for this lab, and never commit them to git.

Terraform's Confluent provider needs a **Cloud API key** (the credential for
managing Confluent Cloud resources, as opposed to the Kafka keys used by
clients). Create one and export it in the shell where you run Terraform. The
secret is shown only once:

```bash
confluent api-key create --resource cloud
export CONFLUENT_CLOUD_API_KEY=<CLOUD_API_KEY>
export CONFLUENT_CLOUD_API_SECRET=<CLOUD_API_SECRET>
```

Choose the AWS region first. **Use the same region for Confluent Cloud and
AWS** (Terraform's `region` variable; the default is `ap-south-1`). Some regions,
such as Hong Kong (`ap-east-1`), are *opt-in* in AWS and are disabled by
default: check with

```bash
aws ec2 describe-regions --all-regions \
  --query "Regions[?RegionName=='<AWS_REGION>'].OptInStatus" --output text
```

`opted-in` or `opt-in-not-required` is fine; `not-opted-in` means you must
enable the region in the AWS console first. Also confirm that Confluent Cloud
supports egress PrivateLink for Enterprise clusters in that region
(`confluent network region list --cloud aws`). Hong Kong (`ap-east-1`) is
opt-in and was not enabled in the account used to test this lab; Mumbai
(`ap-south-1`) worked.

## Running this lab for a class

Enterprise clusters, RDS, and load balancers are expensive, so do not create
them per student. The instructor completes **Part A** (steps 1–3) once. Each
student then does **Part B** (steps 4–6) with a private database, user, and
topic named with their `<STUDENT_ID>`. All names a student creates start with
`<STUDENT_ID>-` or `<student_id>_` so students do not collide.

To let students run the AWS half themselves (for a demonstration, or one stack
per student), the instructor first creates limited AWS credentials with
`lab7/admin-iam` (next section). Each student's stack then needs its own VPC,
and an AWS account allows 5 VPCs per region by default, so for more than a few
students either request a higher quota or have students share one stack.

## Instructor setup: limited AWS credentials for students (once)

Students should not get administrator access to AWS. `lab7/admin-iam` is a small
Terraform configuration, run once **by an AWS administrator**, that creates:

| Resource | Purpose |
| --- | --- |
| IAM policy `lab7-student` | Allows only the EC2/VPC, RDS, load balancer, Route 53 and AMI-lookup actions that `terraform/aws` uses, and nothing else (no IAM, S3, ...) |
| IAM group `lab7-students` | Holds the policy, so a change is made in one place |
| IAM user `lab7-<id>` + access key | One per student; no console password |
| `student-credentials/<id>.txt` | The student's `aws configure set ...` commands (mode 0600, git-ignored) |

The policy also contains **explicit denies**, which always win: only the one
region, only `t3.micro` instances, and only `db.t4g.micro` databases. The files
are commented statement by statement: read `policy.tf` to see exactly what the
lab touches in AWS.

```bash
cd lab7/admin-iam
terraform init
terraform apply -var 'students=["s01","s02","s03","s04","s05","s06","s07","s08","s09","s10","s11","s12","s13","s14","s15","s16","s17","s18","s19","s20","s21","s22","s23","s24","s25","s26"]'
```

Give each student their own file privately. Their credentials do not expire, so
remove them when the class ends:

```bash
terraform destroy -var 'students=["s01","s02","s03","s04","s05","s06","s07","s08","s09","s10","s11","s12","s13","s14","s15","s16","s17","s18","s19","s20","s21","s22","s23","s24","s25","s26"]'
```

Re-running `apply` with a longer list adds students. The state file holds the
secret keys: keep it private (it is git-ignored). Because this account is
shared, the policy cannot stop a student from deleting another student's
lab7 resources in the same region: it limits *what kind* of thing and *where*,
not *whose*. Use a separate AWS account per student if you need that.

## Part A: Build the private path (instructor, once)

### 1. Read the Terraform before you run it

The Terraform is split in two directories so that you can see which half does
what, and so that each half needs only its own credentials:

| Directory | Credentials it needs | Builds |
| --- | --- | --- |
| `terraform/confluent` | Confluent Cloud API key | Environment, gateways, Enterprise cluster, service accounts, API keys, and the two Confluent endpoints |
| `terraform/aws` | The student's AWS credentials | VPC, private database, jump host, load balancer, endpoint service, VPC endpoint, private DNS |

The two halves need three values from each other (they are the "connection"
between Confluent Cloud and AWS), and Terraform cannot pass them between separate
directories, so **you pass them by hand**. That makes the order visible:

```
1. terraform/confluent   gateways + cluster      -> egress_principal_arn, ingress_service_name
2. terraform/aws         everything in AWS        -> egress_endpoint_service_name, ingress_vpc_endpoint_id
3. terraform/confluent   apply again with those   -> the egress and ingress endpoints
```

Each file starts with comments that explain what it creates and which manual
command it replaces. Read them in this order:

| File | What it creates | Manual equivalent |
| --- | --- | --- |
| `confluent/versions.tf`, `variables.tf` | The Confluent provider; inputs (region, students, and the two AWS values) | n/a |
| `confluent/confluent.tf` | Environment, egress and ingress gateways, Enterprise cluster, service accounts, role binding, API keys | `confluent environment create`, `network gateway create`, `kafka cluster create`, `iam service-account create`, `api-key create` |
| `confluent/access_points.tf` | Confluent's egress and ingress endpoints (created in the second apply) | `confluent network access-point private-link egress-endpoint create`, `... ingress-endpoint create` |
| `confluent/outputs.tf` | The values you need afterwards (IDs, hosts, keys) | n/a |
| `aws/versions.tf`, `variables.tf` | The AWS provider; inputs (region, your IP, students, and the Confluent values) | n/a |
| `aws/network.tf` | VPC, 3 private subnets, 1 public subnet, internet gateway | `aws ec2 create-vpc`, `create-subnet`, ... |
| `aws/database.tf` | Private RDS PostgreSQL, its firewall, generated passwords, a lookup of its private IP | `aws rds create-db-instance --no-publicly-accessible` |
| `aws/jump.tf` | SSH key pair and a jump host that installs clients and seeds the database | `aws ec2 run-instances` |
| `aws/egress.tf` | Internal NLB, target, VPC endpoint service | `aws elbv2 ...`, `aws ec2 create-vpc-endpoint-service-configuration` |
| `aws/ingress.tf` | VPC endpoint to Confluent, Route 53 private zone and wildcard record | `aws ec2 create-vpc-endpoint`, `aws route53 ...` |
| `aws/outputs.tf` | Values to pass back to Confluent, host names, passwords | n/a |

`terraform/README.md` describes the same order and has a dependency diagram.
Four points are worth understanding before you apply:

- **Why a load balancer:** PrivateLink can only publish a Network Load Balancer,
  and RDS cannot be published directly, so the NLB forwards port 5432 to the
  database's private IP. After an RDS failover or restore that IP can change,
  and the connector then fails until `terraform apply` refreshes it.
- **Which direction:** *egress* is Confluent Cloud connecting out to your
  database; *ingress* is your VPC connecting in to the private Kafka cluster.
  They are separate gateways and endpoints.
- **Who is allowed:** the endpoint service accepts only the AWS principal that
  Confluent assigned to your egress gateway, so no one else can connect.
- **Two destroy-order helpers** (`terraform_data` resources in
  `confluent/confluent.tf` and `aws/egress.tf`) exist because Confluent and AWS
  finish deleting things at different times. They do nothing during `apply`.

### 2. Apply, in three stages

**Stage 1: Confluent Cloud** (about 2 minutes). Needs the Cloud API key from the
prerequisites.

```bash
cd lab7/terraform/confluent
echo 'students = ["s01", "s02", "s03", "s04", "s05", "s06", "s07", "s08", "s09", "s10", "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21", "s22", "s23", "s24", "s25", "s26"]' > terraform.tfvars
terraform init
terraform apply
```

`apply` shows the plan (about 12 resources) and asks you to type `yes`. The
`students` list takes student IDs (lowercase letters and digits). Print the two
values the AWS half needs:

```bash
terraform output egress_principal_arn      # the AWS identity of Confluent's egress gateway
terraform output ingress_service_name      # the endpoint service Confluent published for ingress
```

**Stage 2: AWS** (about 6 minutes, mostly the database). Use the student
credentials. Put your public IP, the same students, and the two values above in
`terraform.tfvars`, then apply:

```bash
cd ../aws
cat > terraform.tfvars <<EOF
admin_cidr                     = "$(curl -s https://checkip.amazonaws.com)/32"
students                       = ["s01", "s02", "s03", "s04", "s05", "s06", "s07", "s08", "s09", "s10", "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21", "s22", "s23", "s24", "s25", "s26"]
confluent_egress_principal_arn = "$(terraform -chdir=../confluent output -raw egress_principal_arn)"
confluent_ingress_service_name = "$(terraform -chdir=../confluent output -raw ingress_service_name)"
EOF
terraform init
terraform apply
```

Read the plan (about 29 resources) before you type `yes`. Use only your own
public IP in `admin_cidr`, so that only you can SSH to the jump host. Stage 2 is
the only stage that needs AWS credentials.

**Stage 3: Confluent endpoints** (about 3 minutes). Give Confluent the two AWS
values and apply again. Only the two endpoints are created (about 2 resources):

```bash
cd ../confluent
cat >> terraform.tfvars <<EOF
egress_endpoint_service_name = "$(terraform -chdir=../aws output -raw egress_endpoint_service_name)"
ingress_vpc_endpoint_id      = "$(terraform -chdir=../aws output -raw ingress_vpc_endpoint_id)"
EOF
terraform apply
```

The outputs are in two places. In `terraform/confluent`:

```bash
terraform output
terraform output -json student_kafka_api_keys
terraform output -json admin_kafka_api_key
```

In `terraform/aws`:

```bash
terraform output
terraform output -json student_db_passwords
terraform output -raw db_admin_password
```

| Output | Directory | Used for |
| --- | --- | --- |
| `environment_id`, `kafka_cluster_id` | confluent | The `--environment` and `--cluster` flags |
| `connector_host` | confluent | `connection.host` in the connector (step 5). Not the RDS host. Empty until stage 3. |
| `student_kafka_api_keys` | confluent | Each student's Kafka key, with the service account ID. It has **no permissions yet**. |
| `admin_kafka_api_key` | confluent | The instructor key for creating topics and ACLs (step 4) |
| `rds_host` | aws | The real database name; reachable only inside the VPC |
| `ssh_command` | aws | Logging in to the jump host (the key is saved as `terraform/aws/lab7-key.pem`) |
| `student_db_passwords` | aws | Each student's read-only database password |
| `db_admin_password` | aws | For changing data in step 6 |

### 3. Check the private path from the jump host

Log in with the `ssh_command` output of `terraform/aws`. Confirm that the seed script finished, that the
database has a private address, and that the private Kafka cluster now resolves
and answers (HTTP 401 without credentials proves the path works):

```bash
ls /var/log/lab7-seeded
getent hosts <RDS_HOST>
dig +short <KAFKA_CLUSTER_ID>.<AWS_REGION>.aws.private.confluent.cloud
curl -s -o /dev/null -w '%{http_code}\n' \
  https://<KAFKA_CLUSTER_ID>.<AWS_REGION>.aws.private.confluent.cloud/kafka/v3/clusters/<KAFKA_CLUSTER_ID>/topics
```

Because the cluster is private, **run the Kafka-side commands in the rest of
this lab from the jump host.** The CLI on your laptop cannot create topics or
ACLs. Account-level commands (connectors) still work from your laptop.

> **DNS gotcha:** do not look up the cluster's host name before stage 2
> (`terraform/aws`) has finished. A lookup that fails before the Route 53 record exists is cached
> for up to 15 minutes. Do not use the `...accesspoint.confluent.cloud` domain
> shown on the ingress endpoint for an Enterprise cluster: it did not accept
> connections in testing.

## Part B: Deploy the connector (each student)

### 4. Create the topic and permissions

Terraform (`terraform/confluent`) already created each student's service account and Kafka API key, and
an administrator account (`lab7-admin`, with the `CloudClusterAdmin` role on the
cluster) and key. A new key has **no permissions of its own**: a student key can
do nothing until you grant its service account ACLs. Get the values:

```bash
cd lab7/terraform/confluent
terraform output -json student_kafka_api_keys   # per student: key, secret, service_account
terraform output -json admin_kafka_api_key      # the instructor key
```

Then, **from the jump host**, create each student's topic and grant the account
permissions on names starting with `<STUDENT_ID>-`, using the Kafka REST API and
the administrator key. Set `<SERVICE_ACCOUNT_ID>` to the student's
`service_account` value (it looks like `sa-...`):

```bash
BASE=https://<KAFKA_CLUSTER_ID>.<AWS_REGION>.aws.private.confluent.cloud:443/kafka/v3/clusters/<KAFKA_CLUSTER_ID>

curl -s -u <ADMIN_KAFKA_API_KEY>:<ADMIN_KAFKA_API_SECRET> -X POST "$BASE/topics" \
  -H 'Content-Type: application/json' \
  -d '{"topic_name":"<STUDENT_ID>-jdbc-orders","partitions_count":1}'

for OP in READ WRITE CREATE DESCRIBE DESCRIBE_CONFIGS; do
  curl -s -u <ADMIN_KAFKA_API_KEY>:<ADMIN_KAFKA_API_SECRET> -X POST "$BASE/acls" \
    -H 'Content-Type: application/json' \
    -d '{"resource_type":"TOPIC","resource_name":"<STUDENT_ID>-","pattern_type":"PREFIXED","principal":"User:<SERVICE_ACCOUNT_ID>","host":"*","operation":"'$OP'","permission":"ALLOW"}'
done

for OP in READ DESCRIBE; do
  curl -s -u <ADMIN_KAFKA_API_KEY>:<ADMIN_KAFKA_API_SECRET> -X POST "$BASE/acls" \
    -H 'Content-Type: application/json' \
    -d '{"resource_type":"GROUP","resource_name":"<STUDENT_ID>-","pattern_type":"PREFIXED","principal":"User:<SERVICE_ACCOUNT_ID>","host":"*","operation":"'$OP'","permission":"ALLOW"}'
done
```

Each successful create prints a JSON body (add `-o /dev/null -w '%{http_code}\n'`
to `curl` to print just the status; it should be 201). The topic permissions are
all the managed connector needs; the consumer-group permission is for the
student's own consumer in step 6.

The connector's topic is the table prefix plus the table name. With
`topic.prefix` set to `<STUDENT_ID>-jdbc-` and table `orders`, the topic is
`<STUDENT_ID>-jdbc-orders`, so create that topic first (a managed connector does
not create it). Give each student their Kafka key and secret, their database user
(`<STUDENT_ID>_reader`) and password, and the `connector_host` output.

### 5. Deploy the PostgreSQL source (JDBC) connector

List the plugin and its settings:

```bash
confluent connect plugin describe PostgresSource
```

Create `jdbc-source.json`. It contains passwords, so keep the file private and
delete it during Cleanup. The host is the **endpoint DNS name**, not the RDS
host name, so traffic uses PrivateLink. Use one style of table settings only:
`table.include.list` with the `...column.mapping` settings (the older
`timestamp.column.name` and `incrementing.column.name` cannot be mixed with
`table.include.list`):

```json
{
  "name": "<STUDENT_ID>-jdbc-source",
  "config": {
    "connector.class": "PostgresSource",
    "name": "<STUDENT_ID>-jdbc-source",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "<KAFKA_API_KEY>",
    "kafka.api.secret": "<KAFKA_API_SECRET>",
    "connection.host": "<ENDPOINT_DNS_NAME>",
    "connection.port": "5432",
    "connection.user": "<STUDENT_ID>_reader",
    "connection.password": "<STUDENT_DB_PASSWORD>",
    "db.name": "labdb",
    "ssl.mode": "require",
    "table.include.list": "<STUDENT_ID>.orders",
    "topic.prefix": "<STUDENT_ID>-jdbc-",
    "mode": "timestamp+incrementing",
    "timestamp.columns.mapping": "<STUDENT_ID>.orders:[updated_at]",
    "incrementing.column.mapping": "<STUDENT_ID>.orders:id",
    "output.data.format": "AVRO",
    "poll.interval.ms": "5000",
    "tasks.max": "1"
  }
}
```

Deploy and wait for `RUNNING` (it usually takes three to five minutes):

```bash
confluent connect cluster create --config-file jdbc-source.json \
  --cluster <KAFKA_CLUSTER_ID> --environment <ENVIRONMENT_ID>
confluent connect cluster list --cluster <KAFKA_CLUSTER_ID> --environment <ENVIRONMENT_ID>
confluent connect cluster describe <CONNECTOR_ID> \
  --cluster <KAFKA_CLUSTER_ID> --environment <ENVIRONMENT_ID>
```

If the connector fails, `describe` shows the validation error, for example
"Could not connect to database ... timed out" (wrong host or a path problem)
or a configuration error. Go to Troubleshooting below.

### 6. Verify the data and change a row

Read the topic from the jump host (the cluster is private, and the Avro values
are binary, so this example prints a count and the raw bytes). Install the
client once, then run the consumer with the student's key:

```bash
sudo dnf install -y python3-pip && pip3 install confluent-kafka
```

Save as `consume.py`:

```python
import sys, time
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "<KAFKA_CLUSTER_ID>.<AWS_REGION>.aws.private.confluent.cloud:9092",
    "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
    "sasl.username": sys.argv[1], "sasl.password": sys.argv[2],
    "group.id": sys.argv[3], "auto.offset.reset": "earliest"})
c.subscribe(["<STUDENT_ID>-jdbc-orders"])
n, end = 0, time.time() + 25
while time.time() < end:
    m = c.poll(1.0)
    if m and m.error():
        print("ERROR:", m.error())
    elif m:
        n += 1
        print(m.offset(), m.value()[5:30])
print("records", n)
c.close()
```

```bash
python3 consume.py <KAFKA_API_KEY> <KAFKA_API_SECRET> <STUDENT_ID>-jdbc-reader
```

You should see three records, one per seeded row (the readable parts show
`c-1`, `c-2`, `c-3`). The consumer group name must start with `<STUDENT_ID>-`.
Now, on the jump host, change the data as the database administrator (the
student user is read-only) and run the consumer again with a new group name:

```sql
INSERT INTO s07.orders (customer_id, amount) VALUES ('c-4', 42.00);
UPDATE s07.orders SET amount = 13.00, updated_at = now() WHERE id = 1;
```

Two more records arrive within a few seconds: the new row, and the updated row
as a second record for the same `id` (because the connector uses
`timestamp+incrementing` mode). In the console, open the connector's **Overview**
tab to see the task status and throughput.

Finally, check which path the traffic took. Change `connection.host` to the RDS
host name (`<RDS_HOST>`) and apply it with `confluent connect cluster update
<CONNECTOR_ID> --config-file jdbc-source.json`. The connection attempt times
out, because that address resolves to a private IP that Confluent Cloud cannot
reach. The connector shows the validation error, but keeps running on its
previous working configuration. Change the host back.

## Public alternative (comparison)

Without PrivateLink, a managed connector reaches a database over the internet.
You would make the RDS instance publicly accessible and allow only Confluent
Cloud's egress IP addresses in its security group, which you can list with
`confluent network ip-address list --cloud aws --region <AWS_REGION>`. This
works with Basic and Standard clusters but exposes the database to the
internet, and the egress IP list can change. PrivateLink avoids both problems
and is the usual choice for production databases.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Egress endpoint stays pending | The connection request was not accepted in AWS, or the Confluent principal is not in the endpoint service's allowed principals. |
| Endpoint is `READY` but the connector cannot connect | The NLB target IP no longer matches the database, the target is unhealthy, or the database security group does not allow the VPC CIDR. |
| Authentication failed | Wrong `connection.user` or password, or the user has no `USAGE` on the schema or `SELECT` on the table. |
| Connection timeout only for some tasks or intermittently | The endpoint or NLB subnets do not cover all Availability Zones the gateway uses. Match the AZ IDs. |
| `SSL` errors | The database requires TLS; keep `ssl.mode` set to `require`. |
| `lkc-....private.confluent.cloud` does not resolve, or resolves to nothing | The private hosted zone is missing, is not attached to the VPC, or the lookup happened before the record was `INSYNC` (a failed lookup is cached for up to 15 minutes). |
| TLS connection reset when reaching the cluster | You used the `...accesspoint.confluent.cloud` name. Use the cluster's own `...aws.private.confluent.cloud` name, with a zone for that domain. |
| Connector stays `FAILED` with "Could not connect ... timed out" | The cluster is Basic or Standard (egress endpoints need Enterprise or Dedicated), or `connection.host` is the RDS address rather than the endpoint DNS name. |
| "Cannot mix legacy and new configuration approaches" | Use `table.include.list` together with `timestamp.columns.mapping` and `incrementing.column.mapping`, not `timestamp.column.name` or `incrementing.column.name`. |
| `terraform apply` fails with "error waiting for Kafka API Key ... to sync" ... `no such host` | The API key resources must set `disable_wait_for_ready = true` (already set in `confluent/confluent.tf`): the provider otherwise tries to reach the private cluster from your laptop. |
| `Cannot create Enterprise clusters with low durability` | Add `--availability high` when creating the Enterprise cluster. |
| `Topic authorization failed` or `GROUP_AUTHORIZATION_FAILED` for a student key | The ACLs in step 4 are missing, or the topic or consumer group name does not start with `<STUDENT_ID>-`. |
| Topic not found | The managed connector does not create topics. Create `<topic.prefix><table>` first. |
| No new records after an update | The `updated_at` column must change on every update; otherwise `timestamp+incrementing` mode misses it. |

## Cleanup

Everything this lab created is in Terraform's state, apart from the connectors
the students deployed (they were created with the CLI, not Terraform). Destroy
in the **reverse order** of the build: Confluent first (it removes the two
endpoints, so the AWS side can then be deleted), then AWS.

```bash
cd lab7/terraform/confluent
terraform destroy

cd ../aws
terraform destroy
```

Each asks you to type `yes`; the variables come from `terraform.tfvars`. Together
they take about 6–10 minutes. While they run you will see these steps, which are
the two destroy-order helpers from step 1:

- `deleting connector ...` (confluent): any connector still on the cluster is
  deleted first, because Confluent will not delete a cluster that has connectors.
- `waiting for Confluent to disconnect from vpce-svc-... (1 connection(s))` (aws):
  Confluent reports its endpoint deleted a few minutes before AWS drops the
  connection, and AWS will not delete the endpoint service until it does. This
  message repeats every 20 seconds, which is normal.

If a `terraform destroy` stops with an error, run the same command again; it
resumes where it stopped. (Errors seen in testing: `ExistingVpcEndpointConnections`
and "The operation is not allowed for the cluster in its current state"; both are
timing problems that the helpers handle.) If you run `terraform/aws` destroy
before `terraform/confluent`, the helper just waits longer, and fails after 30
minutes if the Confluent endpoint still exists.

Then remove the credentials and local files, and confirm nothing is left. Each
resource bills by the hour, especially the Enterprise cluster, the database, and
the load balancer:

```bash
confluent api-key delete <CLOUD_API_KEY> --force     # the key you created in Prerequisites
unset CONFLUENT_CLOUD_API_KEY CONFLUENT_CLOUD_API_SECRET
rm -f jdbc-source.json terraform/aws/lab7-key.pem

confluent environment list
confluent kafka cluster list --environment <ENVIRONMENT_ID>   # should fail: environment is gone
aws ec2 describe-vpcs --query 'Vpcs[?IsDefault==`false`].VpcId' --output text
aws rds describe-db-instances --query 'DBInstances[].DBInstanceIdentifier' --output text
aws route53 list-hosted-zones --query 'HostedZones[].Name' --output text
```

## Review questions

1. Why can a managed connector not reach a database that has only a private IP
   address, and how does PrivateLink solve this?
2. Why is a Network Load Balancer needed in front of RDS, and what can go wrong
   with its IP target?
3. What is the difference between an ingress and an egress endpoint?
4. Why does the connector use the endpoint DNS name and not the RDS host name?
5. Why does `timestamp+incrementing` mode need both columns, and what happens
   to a row updated without changing `updated_at`?

## References

- [Confluent Cloud networking: egress PrivateLink](https://docs.confluent.io/cloud/current/networking/aws-platt.html)
- [Confluent CLI: network gateway](https://docs.confluent.io/confluent-cli/current/command-reference/network/gateway/index.html)
- [PostgreSQL Source (JDBC) connector](https://docs.confluent.io/cloud/current/connectors/cc-postgresql-source.html)
- [AWS: VPC endpoint services](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html)
- [AWS: Network Load Balancer IP targets](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html)
