# Lab 3: Provisioning via CLI & Terraform

In this lab, create a Confluent Cloud environment and Basic Kafka cluster with
the Confluent CLI, then recreate the same named configuration with Terraform.
Create a service account and a Kafka-cluster-scoped API key, and inspect and
test the available service endpoints.

Use a sandbox organization. Each learner must create and use their own
environment; do not share an environment with other learners. Before starting,
choose the short, unique student ID assigned by the instructor (for example,
`s07`). Replace every `<STUDENT_ID>` placeholder with that ID. Use it as a
suffix on the environment, cluster, service account, topic, and Terraform
working-directory names so learners do not collide. Instructor note: this lab needs one environment and one Basic cluster per
learner at the same time. The default Confluent Cloud quota is 25 environments
per organization (shared with all existing environments) and 20 clusters per
environment, so raise the quota first (`confluent service-quota list` shows the
applied limits) and have learners complete the Cleanup section before Lab 5.
Creating clusters and
optional services may incur charges. The CLI-created resources are deleted
before Terraform recreates them; do not use an environment or cluster that
contains data you need.

## Learning objectives

- Authenticate to Confluent Cloud with the Confluent CLI.
- Create an environment and Basic Kafka cluster with CLI commands.
- Declare the same environment and cluster in the Confluent Terraform
  provider and provision them with Terraform.
- Create a service account and issue a Kafka API key scoped to the cluster.
- Locate and test Kafka, Kafka REST, Schema Registry, ksqlDB, and Flink
  endpoints when those services are available.

## Prerequisites

- Confluent CLI and Terraform installed (use `install_terraform_ubuntu24.sh`).
- A Confluent Cloud user account with permission to create environments,
  Basic Kafka clusters, service accounts, and API keys.
- A payment method or credits if required by the organization.
- A sandbox AWS `ap-east-1` region available to the organization.

## When do you need an API key?

An API key is a username and password pair for a program that talks to
Confluent Cloud. You sign in as yourself to manage things, and you use an API
key for everything a program does.

**No API key needed** when you use the `confluent` CLI to create environments,
clusters, service accounts, or role bindings (`confluent login` covers that),
or when you use the web console.

**An API key is needed** when a client reads or writes data, or calls a
service's API:

| What you are doing | Which key |
| --- | --- |
| Producing or consuming messages (CLI produce/consume, Python scripts) | A **Kafka cluster** key, scoped to one cluster |
| Calling the Kafka REST API (`curl` to the REST endpoint) | The same Kafka cluster key |
| Using Schema Registry (registering or reading schemas) | A **Schema Registry** key; a Kafka key does not work here |
| Running Terraform | A **Cloud** key (`--resource cloud`); it manages resources, not data |
| Using ksqlDB | A ksqlDB key |

**Who owns the key:**

- A key created without `--service-account` belongs to you and acts with your
  permissions.
- A key created with `--service-account` belongs to that service account. An
  application should use this kind, and the key can do only what the account's
  roles allow.

**Remember:**

- Creating a key does not give it any permissions. A new service-account key
  cannot read topics until you grant it roles.
- The secret is shown only once, so save it when you create the key. If you
  lose it, create a new key.

## 1. Authenticate with the Confluent CLI

Check the CLI version and sign in:

```bash
confluent version
confluent login
```

List environments and select the organization context shown by the CLI:

```bash
confluent environment list
```

Before proceeding check limits on the org. 
```bash
confluent service-quota list
```

CLI login authenticates Cloud management commands. Kafka API keys created
later are separate credentials for Kafka data-plane clients.

## 2. Create an environment and Basic cluster with the CLI

Create and select your own disposable environment, using your unique suffix:

```bash
confluent environment create lab3-provisioning-<STUDENT_ID> --governance-package essentials
confluent environment list
confluent environment use <ENVIRONMENT_ID>
```

Create a single-zone Basic Kafka cluster in that environment:

```bash
confluent kafka cluster create lab3-basic-<STUDENT_ID> \
  --cloud aws \
  --region ap-east-1 \
  --availability single-zone \
  --type basic \
  --environment <ENVIRONMENT_ID>
```

Provisioning can take several minutes. Wait until the cluster status is `UP`,
then record the environment ID, Kafka cluster ID, cloud, region, availability,
and display names:

```bash
confluent kafka cluster list
confluent kafka cluster use <KAFKA_CLUSTER_ID>
confluent kafka cluster describe
```

The next section deletes this CLI-created environment and cluster before
Terraform creates resources with the same names and settings. Verify the
environment and cluster contain only disposable lab resources before deleting
them.

```bash
confluent kafka cluster delete <KAFKA_CLUSTER_ID>
confluent environment delete <ENVIRONMENT_ID>
```

Wait for deletion to complete. If a resource is still deleting, wait and
check the lists again before continuing.

## 3. Recreate the environment and Basic cluster with Terraform

Create a Confluent Cloud API key for the Terraform provider. The provider
requires a Cloud API key and secret; this is different from the Kafka cluster
API key created later:

```bash
confluent api-key create --resource cloud
```

Save the key and secret securely. The secret is shown only when created. Make
the provider credentials available to Terraform in the current shell without
writing them into the Terraform files:

```bash
export CONFLUENT_CLOUD_API_KEY='<CLOUD_API_KEY>'
export CONFLUENT_CLOUD_API_SECRET='<CLOUD_API_SECRET>'
```

Terraform uses this Cloud API key for resource-management operations in
`terraform plan`, `terraform apply`, and `terraform destroy`. It is not a
Kafka client key.

Create an isolated directory for this lab's Terraform state, then save the
configuration as `$HOME/lab3-provisioning-terraform-<STUDENT_ID>/main.tf`:

```bash
mkdir -p "$HOME/lab3-provisioning-terraform-<STUDENT_ID>"
cd "$HOME/lab3-provisioning-terraform-<STUDENT_ID>"
```

```hcl
terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.86.0"
    }
  }
}

provider "confluent" {}

resource "confluent_environment" "lab3" {
  display_name = "lab3-provisioning-<STUDENT_ID>"

  # Matches --governance-package essentials used with the CLI.
  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "basic" {
  display_name = "lab3-basic-<STUDENT_ID>"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = "ap-east-1"
  basic {}

  environment {
    id = confluent_environment.lab3.id
  }
}

output "environment_id" {
  value = confluent_environment.lab3.id
}

output "kafka_cluster_id" {
  value = confluent_kafka_cluster.basic.id
}

output "kafka_bootstrap_endpoint" {
  value = confluent_kafka_cluster.basic.bootstrap_endpoint
}

output "kafka_rest_endpoint" {
  value = confluent_kafka_cluster.basic.rest_endpoint
}
```

Initialize Terraform, inspect the plan, and apply it:

```bash
terraform init
terraform plan
terraform apply
```

Review the plan before applying. It should create one environment and one
single-zone Basic Kafka cluster named `lab3-provisioning-<STUDENT_ID>` and
`lab3-basic-<STUDENT_ID>` in AWS `ap-east-1`. The IDs will differ from the
resources created with the CLI, but the declared configuration is the same.

`terraform init` initializes this working directory and downloads the
Confluent provider. `terraform plan` previews the changes Terraform proposes;
`terraform apply` asks for confirmation and performs them. If you later edit
`main.tf`, run `terraform plan` and review the proposed changes, then run
`terraform apply` from your student-specific Terraform working directory to
apply the updated configuration. Review carefully for resources Terraform
proposes to replace or delete.

For a saved plan that you can inspect before applying, use:

```bash
terraform plan -out=/tmp/lab3-<STUDENT_ID>.tfplan
terraform show /tmp/lab3-<STUDENT_ID>.tfplan
terraform apply /tmp/lab3-<STUDENT_ID>.tfplan
```

Inspect the outputs and wait for the cluster to become ready:

```bash
terraform output
confluent environment list
confluent kafka cluster list
```

Select the Terraform-created environment and cluster in the CLI for the
remaining steps:

```bash
confluent environment use <TERRAFORM_ENVIRONMENT_ID>
confluent kafka cluster use <TERRAFORM_KAFKA_CLUSTER_ID>
confluent kafka cluster describe
```

## 4. Create a service account and cluster-scoped API key

Create a service account for an application that will use this Kafka cluster:

```bash
confluent iam service-account create lab3-app-<STUDENT_ID> \
  --description "Lab 3 application identity"
confluent iam service-account list \
  --display-name lab3-app-<STUDENT_ID>
```

Record its service account ID. Create a Kafka API key scoped to the Terraform
cluster and owned by that service account:

```bash
confluent api-key create \
  --resource <TERRAFORM_KAFKA_CLUSTER_ID> \
  --service-account <SERVICE_ACCOUNT_ID>
```

Save the API key and secret securely; the secret cannot be retrieved later.
The key is scoped to this Kafka cluster, but a scoped key does not grant the
service account topic permissions. Assign the account only the RBAC roles or
ACLs it needs before using it to read or write topic data. Creating a key for
a service account requires `OrganizationAdmin` or `ResourceOwner` on that
service account. This lab creates the service-account key as an application
credential but does not use it in the endpoint tests; those tests use the
separate administrator Kafka key created in Step 5.

## 5. Find and test service endpoints

### Kafka bootstrap endpoint

The Kafka endpoint appears in `confluent kafka cluster describe` and in
`terraform output kafka_bootstrap_endpoint`. It normally has the form
`SASL_SSL://<host>:9092`. To check TCP reachability, use the host without the
scheme:

```bash
nc -vz <KAFKA_HOST> 9092
```

For a Kafka protocol and Kafka REST test, create a separate cluster API key
for the logged-in administrator. This key is not the service-account key
created in step 4:

```bash
confluent api-key create --resource <TERRAFORM_KAFKA_CLUSTER_ID>
```

Use this administrator-owned Kafka key for the produce/consume commands and
Kafka REST request below. It is separate from the service-account key created
in Step 4.

Create a disposable test topic as an authorized administrator, then use this
authorized cluster API key to produce and consume through the Confluent CLI:

```bash
confluent kafka topic create lab3-endpoint-check-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID> \
  --partitions 1

confluent kafka topic produce lab3-endpoint-check-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID> \
  --api-key <AUTHORIZED_KAFKA_API_KEY> \
  --api-secret <AUTHORIZED_KAFKA_API_SECRET>
```

Enter a line and press `Ctrl-D`, then consume it:

```bash
confluent kafka topic consume lab3-endpoint-check-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID> \
  --api-key <AUTHORIZED_KAFKA_API_KEY> \
  --api-secret <AUTHORIZED_KAFKA_API_SECRET> \
  --from-beginning
```

The test topic is removed during cleanup. The service account key from step 4
will not pass this test unless the service account has the required topic and
consumer-group permissions; Step 6 shows how to grant them.

### Kafka REST endpoint

The REST endpoint appears in `confluent kafka cluster describe` and in
`terraform output kafka_rest_endpoint`. It normally uses HTTPS on port 443.
Use the authorized administrator Kafka API key created above; `curl` prompts
for the secret when only the API key is supplied:

```bash
curl --fail-with-body --user <AUTHORIZED_KAFKA_API_KEY> \
  "<KAFKA_REST_ENDPOINT>/kafka/v3/clusters/<TERRAFORM_KAFKA_CLUSTER_ID>/topics"
```

Expected: an HTTP success response containing a Kafka topic list. An HTTP
`401` or `403` means the endpoint was reached, but credentials or permissions
must be checked. The Kafka REST API is a separate HTTPS endpoint from the
Kafka bootstrap endpoint.

### Schema Registry endpoint

Schema Registry is environment-scoped. List its endpoints and inspect the
environment's registry details:

```bash
confluent schema-registry endpoint list \
  --environment <TERRAFORM_ENVIRONMENT_ID>
confluent schema-registry cluster describe \
  --environment <TERRAFORM_ENVIRONMENT_ID>
```

If the environment has a Schema Registry endpoint, create a separate
Schema Registry API key. A Kafka API key cannot authenticate to Schema
Registry:

```bash
confluent api-key create --resource <SCHEMA_REGISTRY_CLUSTER_ID>
```

Test the endpoint with that Schema Registry key:

```bash
curl --fail-with-body --user <SCHEMA_REGISTRY_API_KEY> \
  "<SCHEMA_REGISTRY_ENDPOINT>/subjects"
```

Expected: an HTTP success response containing a JSON array of subjects (it can
be empty). If the environment has no Schema Registry endpoint, record it as
not provisioned and skip this authenticated check.

### ksqlDB endpoint, if provisioned

ksqlDB is a separate service; it is not created automatically with the Basic
Kafka cluster. Check whether this environment has a ksqlDB cluster:

```bash
confluent ksql cluster list --environment <TERRAFORM_ENVIRONMENT_ID>
```

If one is listed, copy its endpoint and create an API key for the ksqlDB
cluster itself:

```bash
confluent api-key create --resource <KSQL_CLUSTER_ID>
```

Check its `/info` health endpoint with the ksqlDB-specific key:

```bash
curl --fail-with-body --user <KSQL_API_KEY> "<KSQL_ENDPOINT>/info"
```

If no ksqlDB cluster is listed, record the endpoint as not provisioned. Do not
create a ksqlDB cluster just for this check unless its additional cost is
approved.

### Flink endpoint, if available

Flink endpoints are regional and Flink compute pools are separate from Kafka
clusters. List the available Flink regions, endpoints, and pools:

```bash
confluent flink region list
confluent flink endpoint list
confluent flink compute-pool list \
  --environment <TERRAFORM_ENVIRONMENT_ID>
```

If the environment has a compute pool, its ID and status appear in the pool
list. Use the selected pool to check a Flink SQL session:

```bash
confluent flink shell \
  --environment <TERRAFORM_ENVIRONMENT_ID> \
  --compute-pool <FLINK_COMPUTE_POOL_ID>
```

Exit with `Ctrl-D`. If no pool is listed, record that Flink has no compute
resource in this environment. The endpoint list alone shows regional endpoint
availability; it does not mean this environment has a running compute pool.
Provisioning a pool can incur additional charges.

## 6. Add and remove permissions for an existing key

A key has no permissions of its own: it can do what its owner can do. To
change what the service-account key from Step 4 can do, change the service
account's permissions. The key itself stays the same and you do not recreate
it. A Basic cluster does not support granular RBAC, so this lab uses Kafka
ACLs.

First, show that the key is denied. Produce to the Step 5 test topic with the
**service-account** key (not the administrator key):

```bash
confluent kafka topic produce lab3-endpoint-check-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID> \
  --api-key <SERVICE_ACCOUNT_KAFKA_API_KEY> \
  --api-secret <SERVICE_ACCOUNT_KAFKA_API_SECRET>
```

Expected: an authorization error (the key authenticates, but the account has
no permission to write the topic).

Add write permission (Kafka treats `write` and `read` as also allowing `describe`), using your own credentials and the service account ID.
Run it with your logged-in administrator identity, not the service-account key:

```bash
confluent kafka acl create --allow \
  --service-account <SERVICE_ACCOUNT_ID> \
  --operations write \
  --topic lab3-endpoint-check-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID>

confluent kafka acl list \
  --service-account <SERVICE_ACCOUNT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID>
```

Run the same `produce` command again with the same service-account key. It now
succeeds. Reading still fails, because the account was not granted `read`:

```bash
confluent kafka topic consume lab3-endpoint-check-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID> \
  --api-key <SERVICE_ACCOUNT_KAFKA_API_KEY> \
  --api-secret <SERVICE_ACCOUNT_KAFKA_API_SECRET> \
  --from-beginning
```

Add read permission on the topic and on one consumer group, then consume
again:

```bash
confluent kafka acl create --allow \
  --service-account <SERVICE_ACCOUNT_ID> \
  --operations read \
  --topic lab3-endpoint-check-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID>

confluent kafka acl create --allow \
  --service-account <SERVICE_ACCOUNT_ID> \
  --operations read \
  --consumer-group lab3-reader-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID>

confluent kafka topic consume lab3-endpoint-check-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID> \
  --api-key <SERVICE_ACCOUNT_KAFKA_API_KEY> \
  --api-secret <SERVICE_ACCOUNT_KAFKA_API_SECRET> \
  --group lab3-reader-<STUDENT_ID> \
  --from-beginning
```

Now remove the write permission. Use the same options you used to create it:

```bash
confluent kafka acl delete --allow \
  --service-account <SERVICE_ACCOUNT_ID> \
  --operations write \
  --topic lab3-endpoint-check-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID>
```

Repeat the `produce` command: it fails again, with the same key. Consuming
still works because the `read` permissions remain. Permission changes can take a
few seconds to apply, so retry once before assuming something is wrong.

Delete the remaining `read` ACLs in the same way (run `acl list` to see them,
then `acl delete` with matching options) before moving on to Cleanup.

## API key usage summary

| API key | Scope and owner | Where it is used |
| --- | --- | --- |
| Terraform Cloud API key | Cloud resource management; associated with the CLI user | Terraform provider operations such as `plan`, `apply`, and `destroy`. Credentials are provided through `CONFLUENT_CLOUD_API_KEY` and `CONFLUENT_CLOUD_API_SECRET`. |
| Service-account Kafka API key | Terraform-created Kafka cluster; owned by `lab3-app-<STUDENT_ID>` | Created in Step 4 as an application credential. The endpoint tests do not use it because the lab does not grant this service account topic and consumer-group permissions. |
| Administrator Kafka API key | Terraform-created Kafka cluster; associated with the logged-in administrator | Used for the Kafka produce/consume test and the Kafka REST request in Step 5. |
| Schema Registry API key (optional) | Schema Registry cluster | Used only for the Schema Registry REST endpoint check, if Schema Registry is available. |
| ksqlDB API key (optional) | ksqlDB cluster | Used only for the ksqlDB `/info` endpoint check, if a ksqlDB cluster is already provisioned. |

### If you lose an API key secret

Confluent shows an API secret only when the key is created; it cannot be
retrieved later. `confluent api-key list` shows key IDs, not secrets. Create a
replacement key, save its secret, update clients or Terraform to use it, and
verify the replacement works before deleting the old key.

For the Terraform Cloud API key, list and create keys with Cloud scope:

```bash
confluent api-key list --resource cloud
confluent api-key create --resource cloud
```

Update `CONFLUENT_CLOUD_API_KEY` and `CONFLUENT_CLOUD_API_SECRET` to the new
values, then verify Terraform can still access the resources before deleting
the old Cloud key:

```bash
terraform plan
confluent api-key delete <OLD_TERRAFORM_CLOUD_API_KEY_ID>
```

For the service-account Kafka key, list keys for the account and cluster,
then create a replacement with the same scope:

```bash
confluent api-key list \
  --resource <TERRAFORM_KAFKA_CLUSTER_ID> \
  --service-account <SERVICE_ACCOUNT_ID>
confluent api-key create \
  --resource <TERRAFORM_KAFKA_CLUSTER_ID> \
  --service-account <SERVICE_ACCOUNT_ID>
```

After you update any application using this key, delete the old key:

```bash
confluent api-key delete <OLD_SERVICE_ACCOUNT_KAFKA_API_KEY_ID>
```

For the administrator Kafka key, create and list keys scoped to the cluster.
Use the replacement credentials in the Step 5 produce, consume, and Kafka
REST commands before deleting the old key:

```bash
confluent api-key list --resource <TERRAFORM_KAFKA_CLUSTER_ID>
confluent api-key create --resource <TERRAFORM_KAFKA_CLUSTER_ID>
```

After testing the new credentials, delete the old key:

```bash
confluent api-key delete <OLD_ADMIN_KAFKA_API_KEY_ID>
```

## Cleanup

Delete the endpoint test topic and API keys created for this lab. Use the
API-key IDs shown by `confluent api-key list`:

```bash
confluent kafka topic delete lab3-endpoint-check-<STUDENT_ID> \
  --cluster <TERRAFORM_KAFKA_CLUSTER_ID>

confluent api-key list --resource <TERRAFORM_KAFKA_CLUSTER_ID>
confluent api-key list --resource <TERRAFORM_KAFKA_CLUSTER_ID> \
  --service-account <SERVICE_ACCOUNT_ID>
confluent api-key delete <KAFKA_API_KEY_ID>
confluent api-key delete <SERVICE_ACCOUNT_KAFKA_API_KEY_ID>
```

Delete the Lab 3 service account after confirming its ID:

```bash
confluent iam service-account list
confluent iam service-account list \
  --display-name lab3-app-<STUDENT_ID>
confluent iam service-account delete <SERVICE_ACCOUNT_ID>
```

Also list and delete any Schema Registry or ksqlDB API keys created for the
tests before removing their parent resources:

```bash
confluent api-key list --resource <SCHEMA_REGISTRY_CLUSTER_ID>
confluent api-key delete <SCHEMA_REGISTRY_API_KEY_ID>

confluent api-key list --resource <KSQL_CLUSTER_ID>
confluent api-key delete <KSQL_API_KEY_ID>
```

### Manually delete a Kafka cluster

If you need to remove a cluster manually, first verify its ID and status:

```bash
confluent kafka cluster list
confluent kafka cluster describe <KAFKA_CLUSTER_ID>
```

Then delete it with the Confluent CLI and confirm it no longer appears in the
cluster list:

```bash
confluent kafka cluster delete <KAFKA_CLUSTER_ID>
confluent kafka cluster list
```

The cluster and its data are deleted. For this lab's Terraform-managed
cluster, prefer `terraform destroy` below so Terraform state stays in sync.
Manually deleting a Terraform-managed cluster leaves it recorded in Terraform
state and can cause a later `terraform plan` or `apply` to propose changes.

After cleanup, destroy only the resources in this lab's Terraform workspace:

```bash
terraform destroy
```

This deletes the Terraform-managed Basic cluster and environment. Confirm the
plan contains only `lab3-basic-<STUDENT_ID>` and
`lab3-provisioning-<STUDENT_ID>` before approving the destroy. Then remove the
Cloud API key used by the Terraform provider:

```bash
confluent api-key list --resource cloud
confluent api-key delete <TERRAFORM_CLOUD_API_KEY_ID>
```

Unset the temporary Cloud API credentials afterward:

```bash
unset CONFLUENT_CLOUD_API_KEY CONFLUENT_CLOUD_API_SECRET
```

## Review questions

1. Which credentials authenticate the Confluent CLI, Terraform provider, Kafka
   clients, Kafka REST API, and Schema Registry?
2. How do the CLI-created and Terraform-created resources compare, and why did
   the lab delete the first pair before recreating them?
3. Why does a Kafka API key scoped to a cluster not automatically grant topic
   read or write permission?
4. Which endpoints are available by default with the Basic Kafka cluster, and
   which depend on separately provisioned services?

## References

- [Create a Kafka cluster with the Confluent CLI](https://docs.confluent.io/cloud/current/clusters/create-cluster.html)
- [Confluent Terraform provider for Confluent Cloud](https://docs.confluent.io/cloud/current/clusters/terraform-provider.html)
- [Terraform Kafka cluster resource](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/confluent_kafka_cluster)
- [Test connectivity to Confluent Cloud](https://docs.confluent.io/cloud/current/networking/testing.html)
- [Kafka REST API quick start](https://docs.confluent.io/cloud/current/kafka-rest/krest-qs.html)
- [Schema Registry REST API usage](https://docs.confluent.io/cloud/current/sr/sr-rest-apis.html)
- [ksqlDB on Confluent Cloud](https://docs.confluent.io/cloud/current/cp-component/ksqldb-ccloud-cli.html)
- [Confluent Flink CLI reference](https://docs.confluent.io/confluent-cli/current/command-reference/flink/index.html)
