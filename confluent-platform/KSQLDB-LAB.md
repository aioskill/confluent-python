# Lab: Stream processing with ksqlDB

In this lab you start a ksqlDB server against the SASL/SCRAM-secured Confluent
Platform cluster (sa01, sa02, sa03), and use SQL to filter, join and aggregate a
stream of page views. At the end you write the results in Avro and look at the
schema in Schema Registry.

Duration: about 75 minutes.

What you will do:

1. Start a ksqlDB server on sa01.
2. Create a stream and a table on Kafka topics, and load data with `INSERT`.
3. Run push queries and pull queries.
4. Start persistent queries: filter, stream-table join, count, windowed count.
5. Write a stream in Avro and see the schema in Schema Registry.
6. Inspect, stop and clean up everything.

All commands run on sa01 (`ssh ubuntu@sa01`). Topics created by ksqlDB use one
partition.

## Concepts used in this lab

| Concept | Meaning |
|---|---|
| Stream | An unbounded sequence of events (every row is an insert). Backed by a Kafka topic. |
| Table | The latest value per key (every row is an upsert). Backed by a compacted topic. |
| Push query | `SELECT ... EMIT CHANGES`. Keeps running and pushes new rows to the client. |
| Pull query | `SELECT ...` on a table, with a key in `WHERE`. Returns the current answer and ends. |
| Persistent query | `CREATE STREAM/TABLE ... AS SELECT`. Runs on the server until you terminate it and writes its result to a new topic. |

## Part 0: Start the ksqlDB server

ksqlDB is installed in `/app/confluent/bin` but no service runs it. You start one
server with its own settings file in `/home/ubuntu/ksqldb-lab`. The shipped file
`/app/confluent/etc/ksqldb/ksql-server.properties` stays unchanged.

The Kafka client settings (SASL/SCRAM) are the ones of
`/app/confluent/etc/kafka/client.properties`. The command below reads the password
from that file, so it is not typed anywhere.

```bash
ssh ubuntu@sa01
mkdir -p /home/ubuntu/ksqldb-lab
umask 077
cat > /home/ubuntu/ksqldb-lab/ksql-server.properties <<EOF
listeners=http://0.0.0.0:8088
bootstrap.servers=sa01:9092,sa02:9092,sa03:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="confluent" password="$(sed -n 's/.*password="\([^"]*\)".*/\1/p' /app/confluent/etc/kafka/client.properties)";
ksql.schema.registry.url=http://sa01:8081
ksql.service.id=ksql_lab_
ksql.internal.topic.replicas=3
ksql.streams.replication.factor=3
ksql.streams.state.dir=/home/ubuntu/ksqldb-lab/state
ksql.logging.processing.topic.auto.create=true
ksql.logging.processing.stream.auto.create=true
ksql.logging.processing.topic.replication.factor=3
EOF
```

What the settings do:

- `bootstrap.servers`, `security.protocol`, `sasl.*`: how ksqlDB (and the Kafka Streams
  applications it runs) log in to Kafka. Without them every query fails to connect.
- `ksql.schema.registry.url`: used for the Avro, Protobuf and JSON Schema formats.
- `ksql.service.id`: prefix of all the topics and consumer groups ksqlDB creates
  (`_confluent-ksql-ksql_lab_...`). Servers with the same id form one cluster.
- `ksql.internal.topic.replicas` and `ksql.streams.replication.factor`: replication
  factor 3 for the command topic, the changelog topics and the output topics.
- `ksql.logging.processing.*`: ksqlDB writes rows that it could not process (for
  example bad JSON) into the stream `KSQL_PROCESSING_LOG`.

Start the server in the background and check it:

```bash
LOG_DIR=/home/ubuntu/ksqldb-lab/logs nohup /app/confluent/bin/ksql-server-start \
  /home/ubuntu/ksqldb-lab/ksql-server.properties > /home/ubuntu/ksqldb-lab/server.out 2>&1 &
sleep 40
curl -s http://sa01:8088/info
```

Expected: `"serverStatus":"RUNNING"`. The server log shows errors about
`version-check.confluent.io`: the lab network has no internet access, so the usage
report cannot be sent. Ignore them.

Open the ksqlDB command line client. Every statement in the exercises is typed there
and ends with `;`:

```bash
/app/confluent/bin/ksql http://sa01:8088
```

`exit` leaves the client; the server and its queries keep running.

Run this once in every new client session, so that queries read topics from the start:

```sql
SET 'auto.offset.reset'='earliest';
```

## Exercise 1: A stream and a table

Create a stream of page views and a table of users. ksqlDB creates the two Kafka
topics (1 partition) because they do not exist.

The `TIMESTAMP` property tells ksqlDB to use a column as the event time of the row
(and as the timestamp of the Kafka record), instead of the time of the insert. We need
this to place the events in known minutes.

```sql
CREATE STREAM pageviews (
  viewtime BIGINT,
  user_id VARCHAR,
  page_id VARCHAR
) WITH (KAFKA_TOPIC='ksql_pageviews', PARTITIONS=1, VALUE_FORMAT='JSON', TIMESTAMP='viewtime');

CREATE TABLE users (
  user_id VARCHAR PRIMARY KEY,
  region VARCHAR,
  level VARCHAR,
  updated BIGINT
) WITH (KAFKA_TOPIC='ksql_users', PARTITIONS=1, VALUE_FORMAT='JSON', TIMESTAMP='updated');
```

Load the data. `viewtime` and `updated` are epoch milliseconds. Every time below is
`T0` plus an offset, where `T0 = (UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000` is the start of the hour two
hours ago. `T0` is computed when each statement runs, so run the inserts together (if
the clock passes a full hour while you run them, drop the objects as in exercise 9
and start again). Fixed old dates would not work: Kafka deletes records older than the
topic retention (7 days), which includes records whose `TIMESTAMP` is old.

```sql
INSERT INTO users (user_id, region, level, updated) VALUES ('u1', 'EU', 'gold', (UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 - 600000);
INSERT INTO users (user_id, region, level, updated) VALUES ('u2', 'US', 'silver', (UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 - 600000);
INSERT INTO users (user_id, region, level, updated) VALUES ('u3', 'APAC', 'gold', (UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 - 600000);

INSERT INTO pageviews (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 0, 'u1', 'home');
INSERT INTO pageviews (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 10000, 'u2', 'checkout');
INSERT INTO pageviews (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 20000, 'u1', 'checkout');
INSERT INTO pageviews (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 30000, 'u3', 'home');
INSERT INTO pageviews (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 70000, 'u1', 'cart');
INSERT INTO pageviews (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 80000, 'u2', 'home');
INSERT INTO pageviews (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 140000, 'u1', 'checkout');
```

Look at what exists:

```sql
SHOW STREAMS;
SHOW TABLES;
SHOW TOPICS;
DESCRIBE pageviews;
DESCRIBE users;
```

Questions:

1. Which topic backs each object, and how many partitions and replicas do the topics have?
2. What is the key of `users`? What is the key of `pageviews`? (`DESCRIBE` marks the
   key of a table as `(primary key)`; `pageviews` was created without a key column.)

## Exercise 2: Push queries

A push query never ends by itself. `LIMIT` stops it after a number of rows (without
`LIMIT` press Ctrl+C).

```sql
SELECT user_id, page_id FROM pageviews EMIT CHANGES LIMIT 7;
SELECT * FROM users EMIT CHANGES LIMIT 3;
SELECT user_id, page_id FROM pageviews WHERE page_id = 'checkout' EMIT CHANGES LIMIT 3;
```

Questions:

1. Why does the first query return the rows you inserted earlier? (Hint: `auto.offset.reset`.)
2. Run `SET 'auto.offset.reset'='latest';` and the first query again without `LIMIT`.
   Why is nothing returned? Insert one more page view from a second client session
   (`ksql http://sa01:8088`), watch it appear, then press Ctrl+C and
   `SET 'auto.offset.reset'='earliest';` again. Use a `viewtime` of
   `(UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 200000`.

## Exercise 3: A persistent query (filter)

`CREATE STREAM ... AS SELECT` starts a query on the server. It reads `pageviews`
continuously and writes every matching row to a new topic, even after your client
has closed.

```sql
CREATE STREAM checkout_views AS
  SELECT user_id, page_id, viewtime FROM pageviews WHERE page_id = 'checkout' EMIT CHANGES;

SHOW QUERIES;
SELECT * FROM checkout_views EMIT CHANGES LIMIT 3;
```

Questions:

1. What is the ID of the query and what is its status?
2. Which Kafka topic receives the result? Check with `SHOW TOPICS;`.
3. Insert another checkout (`viewtime` `(UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 160000`, `u3`, `checkout`). Does `checkout_views`
   get it without starting anything?

## Exercise 4: Join a stream with a table

Add the region and level of the user to every page view. A stream-table join looks up,
for each page view, the row of the table that was valid at the time of that page view.

```sql
CREATE STREAM pageviews_enriched AS
  SELECT p.user_id AS user_id, p.page_id, u.region, u.level
  FROM pageviews p LEFT JOIN users u ON p.user_id = u.user_id
  EMIT CHANGES;

SELECT user_id, page_id, region, level FROM pageviews_enriched EMIT CHANGES LIMIT 7;
```

Change a user, then view the same user again. The table row of `u2` is replaced with a
time after all existing page views, so only page views after it see `gold`:

```sql
INSERT INTO users (user_id, region, level, updated) VALUES ('u2', 'US', 'gold', (UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 300000);
INSERT INTO pageviews (viewtime, user_id, page_id) VALUES ((UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 310000, 'u2', 'cart');

SELECT user_id, page_id, level FROM pageviews_enriched WHERE user_id = 'u2' EMIT CHANGES LIMIT 3;
```

Questions:

1. Why do the old page views of `u2` still show `silver`?
2. Why are `pageviews` and `users` required to have the same number of partitions?
3. What would happen to the join result if `users` rows had no `TIMESTAMP` and
   were inserted now (the time of the insert is later than 2026-01-01)?
   Try it: this is why the table above has an `updated` column.

## Exercise 5: Aggregations and pull queries

```sql
CREATE TABLE views_per_user AS
  SELECT user_id, COUNT(*) AS views FROM pageviews GROUP BY user_id EMIT CHANGES;

CREATE TABLE views_per_minute AS
  SELECT page_id, COUNT(*) AS views FROM pageviews
  WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY page_id EMIT CHANGES;
```

Wait about 10 seconds so the queries can process the existing rows, then run the pull
queries. A pull query reads the materialized state of the table and returns at once:

```sql
SELECT * FROM views_per_user WHERE user_id = 'u1';
SELECT * FROM views_per_user;

SELECT page_id,
       TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS window_start,
       views
FROM views_per_minute
WHERE page_id = 'checkout';
```

Watch the count of one user change as a push query. In the second session insert a page
view for `u1` (for example `viewtime` `(UNIX_TIMESTAMP() / 3600000) * 3600000 - 7200000 + 400000`, `home`):

```sql
SELECT user_id, views FROM views_per_user WHERE user_id = 'u1' EMIT CHANGES;
```

Questions:

1. What count does each user have? How many windows has `checkout`, and why is the
   count of one window 2?
2. Where does a pull query on `views_per_user` read the answer from: the Kafka topic or
   a state store of the query? Run `SELECT * FROM pageviews WHERE user_id = 'u1';` too.
   How is it different from `SELECT ... EMIT CHANGES` on the same stream?
3. The windowed table has the cleanup policy `compact,delete`. Why does a windowed
   table need `delete`? (`DESCRIBE views_per_minute EXTENDED;` shows it.)

## Exercise 6: Avro and Schema Registry

The `pageviews` stream is JSON, with no schema stored anywhere. Write a copy in Avro:
ksqlDB registers the schema in Schema Registry (`http://sa01:8081`) for you.

```sql
CREATE STREAM pageviews_avro WITH (KAFKA_TOPIC='ksql_pageviews_avro', VALUE_FORMAT='AVRO', PARTITIONS=1) AS
  SELECT * FROM pageviews EMIT CHANGES;

PRINT 'ksql_pageviews_avro' FROM BEGINNING LIMIT 3;
```

`PRINT` shows the format it detected (`AVRO`) and the value as JSON. Leave the client
(`exit`) and look at the registry and at the raw bytes:

```bash
curl -s http://sa01:8081/subjects
curl -s http://sa01:8081/subjects/ksql_pageviews_avro-value/versions/latest
/app/confluent/bin/kafka-console-consumer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092 --topic ksql_pageviews_avro --from-beginning --max-messages 2 | cat -v
```

The console consumer shows binary: a byte `0`, a 4 byte schema ID and the Avro data.
To see the records decoded, use the Avro consumer:

```bash
/app/confluent/bin/kafka-avro-console-consumer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092 --formatter-property schema.registry.url=http://sa01:8081 \
  --topic ksql_pageviews_avro --from-beginning --max-messages 2
```

Questions:

1. What is the subject name, and what are the fields and types of the schema? (Note
   that ksqlDB makes every field a union with `null`.)
2. The message starts with 5 bytes before the Avro data. What are they, and why does
   the message carry the schema ID and not the schema?
3. Create the stream a second time over the Avro topic, with only a column list
   (`CREATE STREAM pageviews_avro_read WITH (KAFKA_TOPIC='ksql_pageviews_avro', VALUE_FORMAT='AVRO');`).
   Why are no columns needed?

## Exercise 7: Inspect the running queries

Open the client again:

```bash
/app/confluent/bin/ksql http://sa01:8088
```

```sql
SHOW QUERIES;
EXPLAIN CSAS_CHECKOUT_VIEWS_5;
DESCRIBE views_per_user EXTENDED;
SHOW TOPICS;
```

Use the real query ID from `SHOW QUERIES` in `EXPLAIN` (the number depends on how many
statements you ran). `DESCRIBE ... EXTENDED` shows the query that writes the object,
the topic, and the statistics (messages in and out, last message time).

From the shell, the consumer groups of the queries and the internal topics:

```bash
/app/confluent/bin/kafka-consumer-groups \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092 --list | grep ksql_lab_
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092 --list | grep -E 'ksql_lab_|VIEWS|CHECKOUT|ksql_'
```

Questions:

1. Every persistent query is a Kafka Streams application with its own consumer group.
   Which internal topics does the aggregation create (`...-changelog`, `...-repartition`)?
   What is each used for?
2. Which topic is the command topic and what is written to it?

## Exercise 8: A bad record

Write one line that is not valid JSON to the topic of `pageviews`, with the console
producer on sa01:

```bash
echo 'this is not json' | /app/confluent/bin/kafka-console-producer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092 --topic ksql_pageviews
```

In the client, check that the queries are still running and the results did not change:

```sql
SHOW QUERIES;
SELECT * FROM views_per_user;
```

ksqlDB logs every record it cannot deserialize to the server log:

```bash
grep -m1 'Failed to deserialize value from topic: ksql_pageviews' /home/ubuntu/ksqldb-lab/server.out
grep -c 'Failed to deserialize value from topic: ksql_pageviews' /home/ubuntu/ksqldb-lab/server.out
```

Questions:

1. Does a persistent query stop at the bad record? Why is the error logged several
   times for one bad record (how many queries read `ksql_pageviews`)?
2. When is it better to stop on a bad record, and when to skip it? (ksqlDB skips and
   logs. The stream `KSQL_PROCESSING_LOG` is meant to hold these errors as rows, but it
   stays empty in this lab because the server log4j file does not send them to Kafka.)

## Exercise 9: Stop and clean up

A persistent query keeps running until you terminate it, and an object cannot be
dropped while a query reads or writes it. Terminate all queries first and check that
none is left:

```sql
TERMINATE ALL;
SHOW QUERIES;
```

Run `SHOW QUERIES;` again until it returns nothing. Then drop the objects, the ones that
read others last, and delete their topics and schemas with them:

```sql
DROP STREAM pageviews_avro DELETE TOPIC;
DROP TABLE views_per_minute DELETE TOPIC;
DROP TABLE views_per_user DELETE TOPIC;
DROP STREAM pageviews_enriched DELETE TOPIC;
DROP STREAM checkout_views DELETE TOPIC;
DROP TABLE users DELETE TOPIC;
DROP STREAM pageviews DELETE TOPIC;
SHOW STREAMS;
SHOW TABLES;
```

If you created `pageviews_avro_read` in exercise 6, drop it first (without
`DELETE TOPIC`).

Leave the client (`exit`) and stop the server:

```bash
/app/confluent/bin/ksql-server-stop
```

Wait until the server has stopped (`pgrep -f '[K]sqlServerMain'` prints nothing; this
can take 30 seconds). Then remove what ksqlDB left in Kafka. The internal topics and
consumer groups of the lab have the prefix `_confluent-ksql-ksql_lab_`:

```bash
for topic in $(/app/confluent/bin/kafka-topics \
    --command-config /app/confluent/etc/kafka/client.properties \
    --bootstrap-server sa01:9092 --list | grep -E '^(_confluent-ksql-ksql_lab_|ksql_lab_)'); do
  /app/confluent/bin/kafka-topics \
    --command-config /app/confluent/etc/kafka/client.properties \
    --bootstrap-server sa01:9092 --delete --topic "$topic"
done
for group in $(/app/confluent/bin/kafka-consumer-groups \
    --command-config /app/confluent/etc/kafka/client.properties \
    --bootstrap-server sa01:9092 --list | grep '^_confluent-ksql-ksql_lab_'); do
  /app/confluent/bin/kafka-consumer-groups \
    --command-config /app/confluent/etc/kafka/client.properties \
    --bootstrap-server sa01:9092 --delete --group "$group"
done
rm -rf /home/ubuntu/ksqldb-lab/state /home/ubuntu/ksqldb-lab/logs
```

A group that is "not empty" still has members of the stopped server; run the loop again
after one minute.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `curl http://sa01:8088/info` fails | The server is still starting (wait 30 s), or it stopped: read `/home/ubuntu/ksqldb-lab/server.out`. |
| `serverStatus` is `ERROR` | A statement in the command topic cannot run, for example a source topic was deleted while a query used it. Stop the server and follow the cleanup of exercise 9, then start it again. |
| `Could not write the statement ... into the command topic` | The server is not healthy (see above) or is busy stopping queries; wait and retry. |
| A query returns no rows | `auto.offset.reset` is `latest` in this session (run the `SET` of Part 0), or the persistent query has not processed the data yet (wait a few seconds). |
| `Failed to create new KafkaAdminClient` / `Authentication failed` | The SASL settings of `ksql-server.properties` are wrong. Compare with `/app/confluent/etc/kafka/client.properties`. |
| Rows that were there are gone | The records are older than the topic retention (7 days by default) and the broker deleted them. Use timestamps close to now (exercise 1). |
| A push query without `LIMIT` never ends | That is what a push query does. Press Ctrl+C. A client that is killed leaves its query in `SHOW QUERIES` for a while; it is removed when the server notices the connection is gone. |
| `Cannot drop X. The following queries read from this source` | Terminate the queries first and drop the objects in the order of exercise 9. |
| Join result columns are `null` | No table row exists for that key at the time of the stream row (see exercise 4). |
