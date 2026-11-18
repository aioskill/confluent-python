# Lab 6A instructor setup: S3 bucket and shared AWS user

Run this **once, as an AWS administrator**, before Lab 6A. It creates the S3 bucket
that the managed S3 Sink connector writes to and **one IAM user shared by the whole
class**. Students never run this directory.

| File | Contents |
| --- | --- |
| `versions.tf` | Providers (`aws`, `local`) |
| `variables.tf` | Bucket region, how many days objects are kept |
| `s3.tf` | The bucket: private, encrypted, objects expire automatically |
| `iam.tf` | The shared IAM user, its policy and access key, and the credentials file |
| `outputs.tf` | Bucket name, user, credentials file path |

## What the shared key may do

Using the access key in `student-credentials/s3.txt`, the shared user can write, read
and delete objects **only under folders named `s3sink-*/`**, and write one fixed
probe object (`__confluent_cloud_test___`) that the connector uses to prove it can
write. It can also list the bucket: the connector's startup check (HeadBucket)
fails otherwise. Nothing else in AWS is allowed.

Each student writes to their own folder `s3sink-<STUDENT_ID>/` because the lab
tells them to (`topics.dir`). IAM does **not** separate students from each other:
anyone with the key can read or delete another student's folder. Tell students so.

## Run it

The bucket region should match the Kafka cluster's region (Confluent's
recommendation; the lab sets `s3.region` explicitly so a different region works).
`ap-east-1` is opt-in in AWS, so the default is `ap-south-1`.

```bash
terraform init
terraform apply
```

`student-credentials/s3.txt` (mode 0600, git-ignored) holds `s3_bucket`,
`aws_region`, `aws_access_key_id` and `aws_secret_access_key`. Give the same file to
every student privately. The key never expires and state holds its secret
(git-ignored). After class:

```bash
terraform destroy
```

The bucket is emptied and deleted too (`force_destroy`).
