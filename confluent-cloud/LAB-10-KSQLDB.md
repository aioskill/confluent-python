# Lab 10: Stream processing with ksqlDB on Confluent Cloud

In this lab you use a fully managed ksqlDB cluster in Confluent Cloud to filter,
join and aggregate a stream of page views with SQL, write a stream in Avro, and
read the error rows that the processing log keeps.

Duration: about 75 minutes.

All students share one Kafka cluster, one Schema Registry and one ksqlDB cluster.
Replace `<STUDENT_ID>` with the short ID assigned by the instructor (lowercase
letters and digits, for example `s07`) in every name below, so that your streams,
tables and topics do not collide with the ones of the other students. A ksqlDB
cluster has one set of stream and table names for all of its users.

A ksqlDB cluster is billed by the hour while it exists (per CSU). Persistent
queries run until you terminate them, so finish the Cleanup section.

## Learning objectives

- Create streams and tables on Kafka topics, and load data with `INSERT`.
- Run push queries and pull queries, and explain the difference.
- Run persistent queries: a filter, a stream-table join, a count and a windowed
  count.
- Write a stream in Avro and find its schema in Schema Registry.
- Inspect the running queries and find a bad record in the processing log.
- Terminate and drop everything, in the right order.

## Concepts used in this lab

| Concept | Meaning |
|---|---|
| Stream | An unbounded sequence of events (every row is an insert), stored in a Kafka topic. |
| Table | The latest value per key (every row is an upsert), stored in a compacted topic. |
| Push query | `SELECT ... EMIT CHANGES`. Keeps running and pushes new rows to the client. |
| Pull query | `SELECT ...` on a table with a key in `WHERE`. Returns the current answer and ends. |
| Persistent query | `CREATE STREAM/TABLE ... AS SELECT`. Runs on the cluster until you terminate it and writes its result to a topic. |
| CSU | Confluent Streaming Unit, the unit that sizes and bills a ksqlDB cluster. A new cluster needs at least 4. |

## Instructor setup

Done once for the class, by an administrator with the Confluent CLI logged in.
`prepare_common_lab.sh` must have been run for the environment, the Kafka cluster
and the service account first.

1. Create the ksqlDB cluster. The service account is the identity that the
   cluster uses to read and write topics, so it needs the topic ACLs that
   `prepare_common_lab.sh` creates, and the Schema Registry role it grants:

```bash
confluent ksql cluster create ksql_training \
  --credential-identity <SERVICE_ACCOUNT_ID> --csu 4 \
  --cluster <KAFKA_CLUSTER_ID> --environment <ENVIRONMENT_ID>
```

   Provisioning takes a few minutes. Wait until the status is `PROVISIONED`:

```bash
confluent ksql cluster describe <KSQL_CLUSTER_ID> --environment <ENVIRONMENT_ID>
```

   Note the cluster ID (`lksqlc-...`) and the endpoint
   (`https://pksqlc-....confluent.cloud:443`).

2. Create an API key for the ksqlDB cluster for every student and hand it out
   privately:

```bash
confluent api-key create --resource <KSQL_CLUSTER_ID> --environment <ENVIRONMENT_ID> \
  --description "ksqlDB lab <STUDENT_ID>"
```

   The key belongs to the identity that runs the command (or to the one you pass
   with `--service-account`), and that identity must be allowed to use the ksqlDB
   cluster. A key of an organization administrator works. A key of a service
   account **without a role on the cluster is rejected with HTTP 403** ("You are
   forbidden from using this cluster"). The role to give is `KsqlAdmin` on the
   ksqlDB cluster (`confluent iam rbac role-binding create ... --role KsqlAdmin
   --ksql-cluster <KSQL_CLUSTER_ID>`); this call was refused with "Forbidden
   Access" when the lab was prepared, so check it in your organization or give
   the role in the Cloud console.

3. Tell the students the endpoint. The ksqlDB editor in the Cloud console (the
   ksqlDB item of the cluster) can be used instead of the command line client below.

## Student setup

Add the three ksqlDB lines to `./confluent-cloud/.env`
(the Kafka and Schema Registry lines are already there):

```
KSQLDB_ENDPOINT=https://pksqlc-xxxxx.ap-south-1.aws.confluent.cloud:443
KSQLDB_KEY=<ksqlDB API key>
KSQLDB_SECRET=<ksqlDB API secret>
```

`ksql_cloud.py` in the same folder sends statements to the REST API of the cluster.
Check the connection:

```bash
cd ./confluent-cloud
python3 ksql_cloud.py "SHOW STREAMS;"
```

Expected: a table with the stream `KSQL_PROCESSING_LOG` (it exists on every ksqlDB
cluster), and no error. `HTTP 403` means your key is not allowed on the cluster
(ask the instructor); `HTTP 401` means a wrong key or secret.

How the helper works, because it differs from the ksqlDB editor: statements are
separated by `;`, `SELECT` and `PRINT` statements are sent to the query endpoint,
and every query reads topics from the start (the `SET 'auto.offset.reset'` command
is not needed). A push query must end: give it a `LIMIT`.

All statements below are run like this, with the statements between `<<'SQL'` and
`SQL`:

```bash
python3 ksql_cloud.py - <<'SQL'
SHOW TOPICS;
SQL
```

## Exercise 1: A stream and a table

Create a stream of page views and a table of users. ksqlDB creates the two Kafka
topics (1 partition) because they do not exist.

The `TIMESTAMP` property tells ksqlDB to use a column as the event time of the row
(and as the timestamp of the Kafka record), instead of the time of the insert. We
need this to place the events in known minutes.

```bash
python3 ksql_cloud.py - <<'SQL'
CREATE STREAM pageviews_<STUDENT_ID> (
  viewtime BIGINT,
  user_id VARCHAR,
  page_id VARCHAR
) WITH (KAFKA_TOPIC='ksql_pageviews_<STUDENT_ID>', PARTITIONS=1, VALUE_FORMAT='JSON', TIMESTAMP='viewtime');

CREATE TABLE users_<STUDENT_ID> (
  user_id VARCHAR PRIMARY KEY,
  region VARCHAR,
  level VARCHAR,
  updated BIGINT
) WITH (KAFKA_TOPIC='ksql_users_<STUDENT_ID>', PARTITIONS=1, VALUE_FORMAT='JSON', TIMESTAMP='updated');
SQL
```

Load the data. `viewtime` and `updated` are epoch milliseconds. Every time below is
`T0` plus an offset, where `T0 = (UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000` is
the start of the hour two hours ago. `T0` is computed when each statement runs, so run
the inserts together (if the clock passes a full hour while you run them, drop the
objects as in the Cleanup section and start again). Fixed old dates would not work:
Kafka deletes records older than the topic retention (7 days), which includes records
whose `TIMESTAMP` is old.

```bash
python3 ksql_cloud.py - <<'SQL'
INSERT INTO users_<STUDENT_ID> (user_id, region, level, updated) VALUES ('u1', 'EU', 'gold', (UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 - 600000);
INSERT INTO users_<STUDENT_ID> (user_id, region, level, updated) VALUES ('u2', 'US', 'silver', (UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 - 600000);
INSERT INTO users_<STUDENT_ID> (user_id, region, level, updated) VALUES ('u3', 'APAC', 'gold', (UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 - 600000);

INSERT INTO pageviews_<STUDENT_ID> (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 0, 'u1', 'home');
INSERT INTO pageviews_<STUDENT_ID> (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 10000, 'u2', 'checkout');
INSERT INTO pageviews_<STUDENT_ID> (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 20000, 'u1', 'checkout');
INSERT INTO pageviews_<STUDENT_ID> (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 30000, 'u3', 'home');
INSERT INTO pageviews_<STUDENT_ID> (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 70000, 'u1', 'cart');
INSERT INTO pageviews_<STUDENT_ID> (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 80000, 'u2', 'home');
INSERT INTO pageviews_<STUDENT_ID> (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 140000, 'u1', 'checkout');
SQL
```

Look at what exists:

```bash
python3 ksql_cloud.py - <<'SQL'
SHOW STREAMS;
SHOW TABLES;
SHOW TOPICS;
DESCRIBE pageviews_<STUDENT_ID>;
DESCRIBE users_<STUDENT_ID>;
SQL
```

Questions:

1. Which topic backs each object, and how many partitions and replicas do the topics
   have? (Confluent Cloud always uses 3 replicas.)
2. What is the key of `users_<STUDENT_ID>`? What is the key of `pageviews_<STUDENT_ID>`?
   (`DESCRIBE` marks a key column with `KEY`; `pageviews` was created without one.)
3. The stream and table names are shown in capitals, and `SHOW TOPICS` also lists a
   topic that starts with the cluster's topic prefix (`pksqlc-...`) and ends with
   `-processing-log`. What is it for? (You use it in exercise 8.)

## Exercise 2: Push queries

A push query never ends by itself. `LIMIT` stops it after a number of rows.

```bash
python3 ksql_cloud.py - <<'SQL'
SELECT user_id, page_id FROM pageviews_<STUDENT_ID> EMIT CHANGES LIMIT 7;
SELECT * FROM users_<STUDENT_ID> EMIT CHANGES LIMIT 3;
SELECT user_id, page_id FROM pageviews_<STUDENT_ID> WHERE page_id = 'checkout' EMIT CHANGES LIMIT 3;
SQL
```

Questions:

1. Why does the first query return the rows you inserted earlier? (Hint: the helper
   starts every query at the beginning of the topic. In the ksqlDB editor you choose
   this with the `auto.offset.reset` setting.)
2. A push query without `LIMIT` does not end. What does the helper do about it
   (see `--timeout` in `python3 ksql_cloud.py --help`)?

## Exercise 3: A persistent query (filter)

`CREATE STREAM ... AS SELECT` starts a query on the cluster. It reads
`pageviews_<STUDENT_ID>` continuously and writes every matching row to a new topic,
even after your client has closed. The output topic is named explicitly, with your ID.

```bash
python3 ksql_cloud.py - <<'SQL'
CREATE STREAM checkout_views_<STUDENT_ID> WITH (KAFKA_TOPIC='ksql_checkout_views_<STUDENT_ID>', PARTITIONS=1) AS
  SELECT user_id, page_id, viewtime FROM pageviews_<STUDENT_ID> WHERE page_id = 'checkout' EMIT CHANGES;

SHOW QUERIES;
SELECT * FROM checkout_views_<STUDENT_ID> EMIT CHANGES LIMIT 3;
SQL
```

Questions:

1. What is the ID of the query and what is its status? Which Kafka topic receives
   the result?
2. Insert another checkout (`viewtime` `T0 + 160000`, user `u3`, page `checkout`) into
   `pageviews_<STUDENT_ID>` and run the last query with `LIMIT 4`. Does
   `checkout_views_<STUDENT_ID>` get the new row without you starting anything?

## Exercise 4: Join a stream with a table

Add the region and level of the user to every page view. A stream-table join looks up,
for each page view, the row of the table that was valid at the time of that page view.

```bash
python3 ksql_cloud.py - <<'SQL'
CREATE STREAM pageviews_enriched_<STUDENT_ID> WITH (KAFKA_TOPIC='ksql_pageviews_enriched_<STUDENT_ID>', PARTITIONS=1) AS
  SELECT p.user_id AS user_id, p.page_id, u.region, u.level
  FROM pageviews_<STUDENT_ID> p LEFT JOIN users_<STUDENT_ID> u ON p.user_id = u.user_id
  EMIT CHANGES;

SELECT user_id, page_id, region, level FROM pageviews_enriched_<STUDENT_ID> EMIT CHANGES LIMIT 7;
SQL
```

Change a user, then view the same user again. The table row of `u2` is replaced with a
time after all existing page views, so only page views after it see `gold`:

```bash
python3 ksql_cloud.py - <<'SQL'
INSERT INTO users_<STUDENT_ID> (user_id, region, level, updated) VALUES ('u2', 'US', 'gold', (UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 300000);
INSERT INTO pageviews_<STUDENT_ID> (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 310000, 'u2', 'cart');

SELECT user_id, page_id, level FROM pageviews_enriched_<STUDENT_ID> WHERE user_id = 'u2' EMIT CHANGES LIMIT 3;
SQL
```

Questions:

1. Why do the old page views of `u2` still show `silver`?
2. Why are the stream and the table required to have the same number of partitions?
3. What would happen to the join result if `users_<STUDENT_ID>` had no `TIMESTAMP` and
   its rows were inserted now (the time of the insert is later than the page views)?
   This is why the table has an `updated` column.

## Exercise 5: Aggregations and pull queries

```bash
python3 ksql_cloud.py - <<'SQL'
CREATE TABLE views_per_user_<STUDENT_ID> WITH (KAFKA_TOPIC='ksql_views_per_user_<STUDENT_ID>', PARTITIONS=1) AS
  SELECT user_id, COUNT(*) AS views FROM pageviews_<STUDENT_ID> GROUP BY user_id EMIT CHANGES;

CREATE TABLE views_per_minute_<STUDENT_ID> WITH (KAFKA_TOPIC='ksql_views_per_minute_<STUDENT_ID>', PARTITIONS=1) AS
  SELECT page_id, COUNT(*) AS views FROM pageviews_<STUDENT_ID>
  WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY page_id EMIT CHANGES;
SQL
```

Wait about 15 seconds so the queries can process the existing rows, then run the pull
queries. A pull query reads the materialized state of the table and returns at once:

```bash
python3 ksql_cloud.py - <<'SQL'
SELECT * FROM views_per_user_<STUDENT_ID> WHERE user_id = 'u1';
SELECT * FROM views_per_user_<STUDENT_ID>;

SELECT page_id,
       TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS window_start,
       views
FROM views_per_minute_<STUDENT_ID>
WHERE page_id = 'checkout';
SQL
```

Questions:

1. What count does each user have? How many windows has `checkout`, and why does the
   first one have a count of 2?
2. Where does a pull query on `views_per_user_<STUDENT_ID>` read the answer from: the
   Kafka topic or a state store of the query? Run
   `SELECT * FROM pageviews_<STUDENT_ID> WHERE user_id = 'u1';` too. How is it different
   from `SELECT ... EMIT CHANGES` on the same stream?
3. The windowed table has the cleanup policy `compact,delete`. Why does a windowed
   table need `delete`?

## Exercise 6: Avro and Schema Registry

The `pageviews` stream is JSON, with no schema stored anywhere. Write a copy in Avro:
ksqlDB registers the schema in the Schema Registry of the environment for you.

```bash
python3 ksql_cloud.py - <<'SQL'
CREATE STREAM pageviews_avro_<STUDENT_ID> WITH (KAFKA_TOPIC='ksql_pageviews_avro_<STUDENT_ID>', VALUE_FORMAT='AVRO', PARTITIONS=1) AS
  SELECT * FROM pageviews_<STUDENT_ID> EMIT CHANGES;
SQL
```

Wait about 10 seconds, then look at the topic and the registered schema:

```bash
python3 ksql_cloud.py "PRINT 'ksql_pageviews_avro_<STUDENT_ID>' FROM BEGINNING LIMIT 2;"
python3 consume_avro.py ksql_pageviews_avro_<STUDENT_ID> --count 2
```

`PRINT` shows the format it detected (`AVRO`) and the value as JSON. The second command
reads the topic with a Python Avro consumer that gets the schema from Schema Registry.
To see the schema itself, open the subject `ksql_pageviews_avro_<STUDENT_ID>-value` in
the Schema Registry page of the Cloud console, or:

```bash
python3 - <<'PY'
import client_config
from confluent_kafka.schema_registry import SchemaRegistryClient

registry = SchemaRegistryClient(client_config.schema_registry_config())
version = registry.get_latest_version("ksql_pageviews_avro_<STUDENT_ID>-value")
print(version.schema.schema_type, "id", version.schema_id)
print(version.schema.schema_str)
PY
```

Questions:

1. What is the subject name, and what are the fields and types of the schema? (Note
   that ksqlDB makes every field a union with `null`.)
2. A message starts with 5 bytes before the Avro data. What are they, and why does the
   message carry the schema ID and not the schema?
3. Create a stream over the Avro topic with only a column list:
   `CREATE STREAM pageviews_avro_read_<STUDENT_ID> WITH (KAFKA_TOPIC='ksql_pageviews_avro_<STUDENT_ID>', VALUE_FORMAT='AVRO');`
   Why are no columns needed?

## Exercise 7: Inspect the running queries

```bash
python3 ksql_cloud.py - <<'SQL'
SHOW QUERIES;
EXPLAIN CSAS_CHECKOUT_VIEWS_<STUDENT_ID_UPPERCASE>_5;
DESCRIBE views_per_user_<STUDENT_ID> EXTENDED;
SQL
```

Use the real query ID from `SHOW QUERIES` in `EXPLAIN` (the number depends on how many
statements ran on the cluster before yours, and the ID contains your student ID in
capitals). `DESCRIBE ... EXTENDED` shows which query writes the object. In the Cloud
console, the ksqlDB cluster page also shows the flow of streams and queries, and the
Stream Lineage page shows the topics that your queries connect.

Questions:

1. Every persistent query is a Kafka Streams application. Which internal topics does
   the aggregation create? List the topics of the Kafka cluster
   (`python3 manage_topics.py list`): the ones that start with `_confluent-ksql-` are
   internal. What is a changelog topic and what is a repartition topic used for?
2. Who pays for the topics and the CSUs of your queries, and what limits how many
   persistent queries one ksqlDB cluster can run?

## Exercise 8: A bad record and the processing log

Write one line that is not valid JSON to the topic of `pageviews`:

```bash
python3 produce_messages.py ksql_pageviews_<STUDENT_ID> --message 'this is not json'
```

Check that the queries are still running and the results did not change, then read the
error from the processing log. On Confluent Cloud the processing log is a stream that
exists on every ksqlDB cluster:

```bash
python3 ksql_cloud.py - <<'SQL'
SHOW QUERIES;
SELECT * FROM views_per_user_<STUDENT_ID>;
SELECT message->deserializationError->errorMessage AS error
FROM KSQL_PROCESSING_LOG
WHERE message->deserializationError->errorMessage LIKE '%ksql_pageviews_<STUDENT_ID>%'
EMIT CHANGES LIMIT 1;
SQL
```

The processing log is shared by all students of the cluster (and keeps the errors of
earlier runs), so the query only shows the errors that mention the topic of your stream.

Questions:

1. Does a persistent query stop at the bad record? Which stream shows the failure?
2. When is it better to stop on a bad record, and when to skip it?

## Cleanup

A persistent query keeps running (and keeps using the CSUs of the cluster) until you
terminate it, and an object cannot be dropped while a query reads or writes it.
Terminate your queries by ID, so you do not stop the queries of other students (list
them with `SHOW QUERIES;`, `TERMINATE ALL;` would stop every query of the cluster):

```bash
python3 ksql_cloud.py "TERMINATE <QUERY_ID>;"
```

Do this for each of your queries, then check that none of yours is left:

```bash
python3 ksql_cloud.py "SHOW QUERIES;"
```

Then drop the objects, the ones that read others last, and delete their topics and
schemas with them. If you created `pageviews_avro_read_<STUDENT_ID>` in exercise 6, drop
it first, without `DELETE TOPIC`:

```bash
python3 ksql_cloud.py - <<'SQL'
DROP STREAM pageviews_avro_<STUDENT_ID> DELETE TOPIC;
DROP TABLE views_per_minute_<STUDENT_ID> DELETE TOPIC;
DROP TABLE views_per_user_<STUDENT_ID> DELETE TOPIC;
DROP STREAM pageviews_enriched_<STUDENT_ID> DELETE TOPIC;
DROP STREAM checkout_views_<STUDENT_ID> DELETE TOPIC;
DROP TABLE users_<STUDENT_ID> DELETE TOPIC;
DROP STREAM pageviews_<STUDENT_ID> DELETE TOPIC;
SHOW STREAMS;
SHOW TABLES;
SQL
```

The topics are deleted a few seconds later, and the internal topics (`_confluent-ksql-...`)
of the terminated queries are removed shortly after. Check with
`python3 manage_topics.py list`.

Instructor, at the end of the class: delete the ksqlDB cluster and the API keys that
were created for it, so that it stops billing:

```bash
confluent ksql cluster delete <KSQL_CLUSTER_ID> --environment <ENVIRONMENT_ID>
confluent api-key delete <KSQLDB_API_KEY>
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `Set KSQLDB_ENDPOINT, KSQLDB_KEY and KSQLDB_SECRET in .env` | The three lines of "Student setup" are missing in `confluent-cloud/.env`. |
| `HTTP 403: You are forbidden from using this cluster` | The identity that owns the API key has no role on the ksqlDB cluster (instructor setup, step 2). |
| `HTTP 401` | Wrong ksqlDB API key or secret (not the Kafka key of the same file). |
| `Could not reach ksqlDB` | Wrong endpoint, or the cluster is not `PROVISIONED` yet. |
| A pull query returns no rows right after the `CREATE TABLE` | The query has not processed the data yet; wait 10 to 15 seconds. |
| Rows that were there are gone | The records are older than the topic retention (7 days) and were deleted. Use timestamps close to now (exercise 1). |
| `Cannot drop X. The following queries read from this source` | Terminate the queries that read or write it first (Cleanup). |
| `A stream or table with name ... already exists` | Another student, or an earlier run of yours, used the same name. Use your own `<STUDENT_ID>`, or drop your old objects. |
