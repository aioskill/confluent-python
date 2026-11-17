# Lab 6B instructor setup: S3 bucket and limited AWS users

Run this **once, as an AWS administrator**, before Lab 6B. It creates the S3 bucket
that the managed S3 Sink connector writes to and one IAM user per student who can
use only their own prefix of it. Students never run this directory.

| File | Contents |
| --- | --- |
| `versions.tf` | Providers (`aws`, `local`) |
| `variables.tf` | Student IDs, bucket region, how many days objects are kept |
| `s3.tf` | The bucket: private, encrypted, objects expire automatically |
| `iam.tf` | One IAM user, policy and access key per student, and the credential files |
| `outputs.tf` | Bucket name, users, credential file paths |

## What a student may do

Using the access key in their file, a student's IAM user can write, read and
delete objects **only under `<STUDENT_ID>/`**, and write one fixed probe object
(`__confluent_cloud_test___`) that the connector uses to prove it can write. It
can also list the bucket: the connector's startup check (HeadBucket) fails
otherwise, so the key *names* of other students are visible but their contents are
not readable or writable. Nothing else in AWS is allowed.

## Run it

The bucket region should match the Kafka cluster's region (Confluent's
recommendation; the lab sets `s3.region` explicitly so a different region works).
`ap-east-1` is opt-in in AWS, so the default is `ap-south-1`.

```bash
terraform init
terraform apply -var 'students=["s01","s02","s03","s04","s05","s06","s07","s08","s09","s10","s11","s12","s13","s14","s15","s16","s17","s18","s19","s20","s21","s22","s23","s24","s25","s26"]'
```

`student-credentials/<id>.txt` (mode 0600, git-ignored) holds `s3_bucket`,
`s3_prefix`, `aws_region`, `aws_access_key_id`, `aws_secret_access_key`. Give each
student their own file privately. The keys never expire and state holds their
secrets (git-ignored). After class, with the same list:

```bash
terraform destroy -var 'students=[...same list...]'
```

The bucket is emptied and deleted too (`force_destroy`).
