# Lab 13: Monitoring Confluent Cloud with the Metrics API and Prometheus

Confluent Cloud runs the brokers for you, so you watch it through the **Metrics API**.
In this lab you read the metrics of a Kafka cluster with `curl`, generate traffic and see it
in the numbers, scrape the Prometheus export endpoint with a Prometheus server that runs in a
container, and learn the key metrics and the alerts that matter.

Duration: about 60 minutes.

Everything for this lab is in this directory, `./confluent-cloud/lab13`:

| File | What it is |
|---|---|
| `LAB-13-MONITORING.md` | This lab. |
| `make_prometheus_config.sh` | Writes `prometheus.yml` and the secret file from the `.env` file of the parent directory. |
| `alerts.yml` | Three Prometheus alert rules (consumer lag, resource access error, scrape down). |

Paths that start with `./` are relative to the root of the repository (the directory that contains `confluent-cloud`).

All students share one Kafka cluster. Replace `<STUDENT_ID>` with the short ID assigned by the
instructor (lowercase letters and digits, for example `s07`) in the topic and group names.
Metrics are shown for the whole cluster, so you see the traffic of the other students too;
the topic and group names with your ID tell you which numbers are yours.

## Learning objectives

- Explain what the Metrics API is, which resources it covers, and what the export endpoint returns.
- Query the descriptors and the export endpoint, and select one metric or one resource.
- Run Prometheus against Confluent Cloud and write PromQL for throughput, storage,
  connections, requests and consumer lag.
- Name the key metrics of a Kafka cluster and the thresholds you alert on.

## Roles and credentials that are required

A wrong key or a missing role returns a clear HTTP error, so check these first.

| What | Required |
|---|---|
| Key type | A **Cloud API key** (resource `cloud`), created with `confluent api-key create --resource cloud`. A Kafka, Schema Registry or Flink key is rejected. |
| Role of the key's owner | `MetricsViewer` on the organization (read-only). Without it the Metrics API answers `HTTP 403: Request is not authorized to access any resource` (this was tested with a service account). An organization administrator already has access. |
| Who creates the key and the grant | An administrator, for example `OrganizationAdmin`. |
| For the traffic exercise | The Kafka API key of `.env` (already used by `produce_messages.py`) and a topic. |

Grant the role to a service account like this (it is one binding for the whole organization):

```bash
confluent iam rbac role-binding create --principal User:<SERVICE_ACCOUNT_ID> --role MetricsViewer
```

## Instructor setup

1. Create a Cloud API key for the service account and give it `MetricsViewer` (above):

```bash
confluent api-key create --resource cloud --service-account <SERVICE_ACCOUNT_ID> \
  --description "Lab 13 monitoring (Metrics API)"
```

2. Add the key to `./confluent-cloud/.env`, and give the
   students the key and secret privately:

```
CLOUD_API_KEY=<Cloud API key>
CLOUD_API_SECRET=<Cloud API secret>
```

3. Docker is needed on the students' computers (Exercise 4 runs Prometheus in a container).

## How the Metrics API works

| Endpoint (`https://api.telemetry.confluent.cloud`) | Use |
|---|---|
| `/v2/metrics/cloud/descriptors/resources` | The resource types you can monitor: `kafka`, `connector`, `ksql`, `schema_registry`, `compute_pool`, `flink_statement`, `flink_materialized_table`. |
| `/v2/metrics/cloud/descriptors/metrics?resource_type=kafka` | The metrics of a resource type, with their labels. |
| `/v2/metrics/cloud/query` | A JSON query for any metric, with aggregation and time intervals (POST). |
| `/v2/metrics/cloud/export` | The **Prometheus export**: the latest data point of every exportable metric of the resources you name, in the Prometheus text format. Prometheus scrapes this. |

- The data lags the real traffic by a minute or two (more for some metrics), and the export
  endpoint returns only the last data point of each series: scrape it every minute.
- A metric has labels such as `kafka_id`, `topic`, `type` (of a request) and
  `consumer_group_id`. A series appears only when there was data for that combination.
- `confluent_scrape_resource_access_error` is 0 when the resource could be read and 1 when
  it could not (a wrong ID or a missing role).

## Exercise 1: Discover resources and metrics

Use the Cloud API key and secret from the instructor. The commands send them as basic
authentication (the placeholders are the values of `CLOUD_API_KEY` and `CLOUD_API_SECRET`):

```bash
curl -s --user '<CLOUD_API_KEY>:<CLOUD_API_SECRET>' \
  https://api.telemetry.confluent.cloud/v2/metrics/cloud/descriptors/resources \
  | python3 -c "import sys, json; print([r['type'] for r in json.load(sys.stdin)['data']])"
```

```bash
curl -s --user '<CLOUD_API_KEY>:<CLOUD_API_SECRET>' \
  'https://api.telemetry.confluent.cloud/v2/metrics/cloud/descriptors/metrics?resource_type=kafka' \
  | python3 -c "
import sys, json
metrics = json.load(sys.stdin)['data']
print(len(metrics), 'metrics for kafka')
for m in metrics[:12]: print(m['name'].split('/')[-1], '-', m['description'][:70])"
```

Questions:

1. Which resource types can you monitor? Which one would you use for a Flink compute pool?
2. Find `received_bytes`, `sent_bytes`, `retained_bytes`, `active_connection_count` and
   `consumer_lag_offsets` in the list of the cluster metrics (print all 88 instead of 12).
   Which labels does `consumer_lag_offsets` have? (Look at the `labels` of the descriptor.)

## Exercise 2: The Prometheus export endpoint

Export all the metrics of your Kafka cluster in the Prometheus format. The cluster ID is the
first part of the bootstrap server name in `.env` (`lkc-...`):

```bash
curl -s --user '<CLOUD_API_KEY>:<CLOUD_API_SECRET>' \
  'https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.kafka.id=<KAFKA_CLUSTER_ID>' \
  | grep -v '^#' | cut -c1-160 | head -12
```

Ask for one metric only, or two, with the `metric` parameter, and for other resources with
`resource.schema_registry.id` and `resource.compute_pool.id`:

```bash
curl -s --user '<CLOUD_API_KEY>:<CLOUD_API_SECRET>' \
  'https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.kafka.id=<KAFKA_CLUSTER_ID>&metric=io.confluent.kafka.server/retained_bytes' \
  | grep -v '^#'
```

```bash
curl -s --user '<CLOUD_API_KEY>:<CLOUD_API_SECRET>' \
  'https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.compute_pool.id=<COMPUTE_POOL_ID>' \
  | grep '^# TYPE' | head -8
```

Questions:

1. How many metric families does the export of the Kafka cluster contain, and how many of the
   88 metrics of exercise 1 are exported? (Metrics that are not exportable are only in `/query`.)
2. Which line shows that Prometheus could read the cluster?
   (`confluent_scrape_resource_access_error`.) What would you see with a wrong cluster ID?
3. The Flink compute pool export has many more series than the cluster. Which metric families
   does it have? Which one tells you how many CFUs your statements use?

## Exercise 3: Generate traffic and see it in the numbers

Create a topic, write 300 messages to it, and read only 100 of them with a consumer group, so
that 200 messages stay unread:

```bash
cd ./confluent-cloud
python3 manage_topics.py create mon-<STUDENT_ID> --partitions 1
python3 produce_messages.py mon-<STUDENT_ID> --count 300 --interval 0.02
python3 consume_messages.py mon-<STUDENT_ID> --group mon-<STUDENT_ID>-group --count 100
```

The numbers appear after a few minutes. Meanwhile continue with exercise 4, and come back to
the queries of exercise 5.

Questions:

1. Which metric families show the produce, and why do they show the topic and not the client?
   Compare the numbers with what you sent (300 messages of about 20 bytes). In testing the
   metrics were larger than the producer's own count: `received_records` peaked at 1490 and
   `received_bytes` at 49117 for these 300 messages, and `retained_bytes` was 16069. The exact
   reason was not investigated (replication to the 3 replicas and the protocol overhead are
   candidates), so use these metrics for trends and alerts, not as exact message counts.
2. After the consume, how many records should the group `mon-<STUDENT_ID>-group` have in lag?
   (Testing showed exactly 200 for this procedure.)

## Exercise 4: Prometheus against Confluent Cloud

Prometheus scrapes `/v2/metrics/cloud/export` with basic authentication. The script writes the
configuration and the secret file from your `.env` file (the secret is read with
`password_file`, so it is not in the configuration file):

```bash
cd ./confluent-cloud/lab13
./make_prometheus_config.sh
cat prometheus.yml
```

The script scrapes the Kafka cluster of `.env` and the compute pool (`FLINK_COMPUTE_POOL_ID`).
Add a Schema Registry with `--schema-registry <lsrc-...>`. Start Prometheus in a container
(the user option lets it read the secret file; the storage path is a writable directory of the
container):

```bash
docker run -d --name prom-lab13 --user "$(id -u):$(id -g)" -p 9090:9090 \
  -v "$PWD"/prometheus.yml:/etc/prometheus/prometheus.yml:ro \
  -v "$PWD"/alerts.yml:/etc/prometheus/alerts.yml:ro \
  -v "$PWD"/cloud_api_secret:/etc/prometheus/cloud_api_secret:ro \
  prom/prometheus:v3.5.0 --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/tmp/prom-data
```

Wait about 70 seconds for the first scrape (the scrape interval is 1 minute), then check the
target and the alert rules. The same pages are in the browser at <http://localhost:9090/targets>
and <http://localhost:9090/rules>:

```bash
curl -s localhost:9090/api/v1/targets | python3 -c "
import sys, json
t = json.load(sys.stdin)['data']['activeTargets'][0]
print(t['health'], t['scrapeUrl'], t['lastError'])"
curl -s localhost:9090/api/v1/rules | python3 -c "
import sys, json
for g in json.load(sys.stdin)['data']['groups']:
    for r in g['rules']: print(r['name'], r['health'], r['state'])"
```

Questions:

1. What is the health of the target, and which URL does Prometheus call? Where are the
   cluster ID and the credentials in `prometheus.yml`?
2. Why is `honor_timestamps: true` set, and why does the `scrape_interval` have to be 1 minute?
3. Stop the container and start it again, then look at `/targets` for a few seconds: what
   state does the target have before the first scrape?

## Exercise 5: Key metrics and queries

Run the queries in the Prometheus page <http://localhost:9090/graph>, or from the shell. This
function sends one query and prints the series:

```bash
q() { curl -s -G localhost:9090/api/v1/query --data-urlencode "query=$1" | python3 -c "
import sys, json
for s in json.load(sys.stdin)['data']['result']:
    print({k: v for k, v in s['metric'].items() if k in ('topic', 'type', 'consumer_group_id')}, s['value'][1])"; }
```

| What you watch | Metric | PromQL example | What to look for |
|---|---|---|---|
| Ingress throughput | `confluent_kafka_server_received_bytes` | `sum by (topic) (confluent_kafka_server_received_bytes)` | Bytes written in the last interval, per topic. A drop to 0 on a busy topic means producers stopped. Compare with the limits of your cluster type. |
| Egress throughput | `confluent_kafka_server_sent_bytes` | `sum by (topic) (confluent_kafka_server_sent_bytes)` | Bytes read by consumers. Egress much higher than ingress means many readers. |
| Records | `confluent_kafka_server_received_records`, `..._sent_records` | `sum by (topic) (confluent_kafka_server_received_records)` | Message rates, to relate to the bytes (average message size). |
| Storage | `confluent_kafka_server_retained_bytes` | `sum by (topic) (confluent_kafka_server_retained_bytes)` | Data kept per topic (it drives the storage bill); growth without a retention limit. |
| Connections | `confluent_kafka_server_active_connection_count` | `sum(confluent_kafka_server_active_connection_count)` | A sudden rise is a client that reconnects or leaks connections. |
| Requests | `confluent_kafka_server_request_count` | `sum by (type) (confluent_kafka_server_request_count)` | Requests by type (`Produce`, `Fetch`, `ApiVersions`, ...). Many `ApiVersions` or `Metadata` requests mean clients that reconnect. |
| Partitions | `confluent_kafka_server_partition_count` | `confluent_kafka_server_partition_count` | Partitions of the cluster, against the limit of the cluster type. |
| Consumer lag | `confluent_kafka_server_consumer_lag_offsets` | `sum by (consumer_group_id, topic) (confluent_kafka_server_consumer_lag_offsets)` | Records written but not yet read. A lag that grows means consumers cannot keep up. The most important alert for applications. |
| Authentication | `confluent_kafka_server_successful_authentication_count` | `sum(confluent_kafka_server_successful_authentication_count)` | Logins per interval; a sudden change means a new or a broken client. |
| Hot partitions | `confluent_kafka_server_hot_partition_ingress`, `..._egress` | `confluent_kafka_server_hot_partition_ingress` | A partition that gets much more than the others (a bad key). |
| Cluster load | `confluent_kafka_server_cluster_load_percent` (Dedicated clusters) | `max(confluent_kafka_server_cluster_load_percent)` | Above 70% to 80% scale the cluster; this metric exists only for Dedicated clusters. |
| Flink | `confluent_flink_compute_pool_utilization_current_cfus` | `max(confluent_flink_compute_pool_utilization_current_cfus)` | CFUs in use against the limit of the compute pool. |

Run these once the numbers of exercise 3 are in Prometheus:

```bash
q 'sum by (topic) (confluent_kafka_server_retained_bytes)'
q 'sum by (topic) (confluent_kafka_server_received_records)'
q 'sum by (consumer_group_id, topic) (confluent_kafka_server_consumer_lag_offsets)'
q 'sum by (type) (confluent_kafka_server_request_count) > 0'
```

The three alert rules of `alerts.yml` cover the lag, a resource the Metrics API cannot read
and a failed scrape. Look at how the first one is written, and at its state during the lag
(`for: 5m` makes it `pending` first, then `firing`).

Questions:

1. What is the lag of `mon-<STUDENT_ID>-group`, and when did the metric first show it? How
   long after the traffic? (In testing the lag line was in the export within about 3 to 5
   minutes, and the `ConsumerLagHigh` alert was `pending` after that.) The throughput metrics
   (`received_*`, `sent_*`) are a count per minute: they show the produce in one sample and
   are 0 in the next, so look at a range, for example
   `max_over_time(sum(confluent_kafka_server_received_bytes{topic="mon-<STUDENT_ID>"})[30m:1m])`.
2. A topic grows by 1 MB a day and has `retention.ms` of 7 days. How many bytes does
   `retained_bytes` show after a month?
3. Which alert would you add for a Dedicated cluster, and for the Flink compute pool?
   Write the PromQL expressions and thresholds.

## Cleanup

Stop Prometheus, and delete the topic and the consumer group of exercise 3:

```bash
docker rm -f prom-lab13
cd ./confluent-cloud
python3 manage_topics.py delete mon-<STUDENT_ID> --yes
```

The consumer group disappears after it has been empty for a while. Remove the generated
files `prometheus.yml` and `cloud_api_secret` of this directory if you do not need them.

Instructor, at the end of the class: delete the Cloud API key
(`confluent api-key delete <CLOUD_API_KEY>`) and, if the service account was only for the
labs, the `MetricsViewer` binding (`confluent iam rbac role-binding delete`).

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `HTTP 401` | Wrong key or secret, or not a Cloud API key. |
| `HTTP 403: Request is not authorized to access any resource` | The owner of the key has no `MetricsViewer` role (see Roles). |
| `confluent_scrape_resource_access_error ... 1` | The resource ID is wrong, or the key's owner cannot see that resource. |
| Prometheus exits with `Unable to create mmap-ed active query log` | The container user cannot write the default data directory. Keep `--storage.tsdb.path=/tmp/prom-data` and `--user`. |
| Prometheus cannot read the secret file (`permission denied`) | The file `cloud_api_secret` has mode 600 and belongs to you; start the container with `--user "$(id -u):$(id -g)"`. |
| The target stays `unknown` | Wait for the first scrape (up to 1 minute after the start). |
| A metric of your topic is missing | The data lags the traffic by a few minutes. A series exists only for combinations that had data. |
| No `consumer_lag_offsets` | The group must have committed offsets and have unread records. |
| `cluster_load_percent` is missing | It exists only for Dedicated clusters. |
