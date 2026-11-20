# Lab 11: Stream processing with Flink SQL on Confluent Cloud

In this lab you use Confluent Cloud for Apache Flink to filter, join and aggregate a
stream of page views with SQL. Flink reads and writes the Kafka topics of your cluster as
tables, and keeps the schemas in Schema Registry. You also see why a window stays open
until a later event arrives, and how to stop and clean up your statements.

Duration: about 75 minutes.

All students share one Kafka cluster, one Schema Registry and one Flink compute pool.
Replace `<STUDENT_ID>` with the short ID assigned by the instructor (lowercase letters and
digits, for example `s07`) in every table and statement name below, so that your objects do
not collide with the ones of the other students. A table you create is also a Kafka topic
with the same name, so the Kafka cluster is shared too.

A compute pool bills for the CFUs that your running statements use. A statement that you do
not stop keeps running and keeps billing, so finish the Cleanup section.

## Learning objectives

- Create tables on Kafka topics (JSON and Avro) and load rows with `INSERT`.
- Run bounded queries, and explain the difference from a continuous statement.
- Start continuous statements: a filter, a temporal join, a windowed count and a count
  per key.
- Explain event time, watermarks and why a window waits for a later event.
- Find the schema that Flink registered in Schema Registry.
- Inspect, stop and delete statements and tables.

## Concepts used in this lab

| Concept | Meaning |
|---|---|
| Table | A Kafka topic plus its schema. Creating a table creates the topic and the schema subject, and dropping it deletes both. |
| Catalog / database | The environment is the catalog and the Kafka cluster is the database. `SHOW TABLES` lists the topics of the cluster. |
| Statement | A SQL command that Flink runs. A DDL or `SELECT ... LIMIT` statement ends quickly; an `INSERT`, `CREATE TABLE ... AS SELECT` or an unbounded `SELECT` keeps running until you stop it. |
| Event time and watermark | The time stored in the row, and Flink's estimate that no older rows will arrive. A window closes when the watermark passes its end. |
| Append and upsert/retract tables | A filter only adds rows (append). A count per key updates earlier rows (changelog). |
| CFU | Confluent Flink Unit. The compute pool has a maximum number of CFUs, shared by all statements. |

## Instructor setup

Done once for the class, with the Confluent CLI logged in as an administrator.
`prepare_common_lab.sh` must have been run for the environment and the Kafka cluster.

1. Find or create a compute pool in the region of the Kafka cluster:

```bash
confluent flink compute-pool list --environment <ENVIRONMENT_ID>
confluent flink compute-pool create training-flink-pool --cloud aws --region ap-south-1 --max-cfu 10 --environment <ENVIRONMENT_ID>
```

   Note the pool ID (`lfcp-...`). An environment normally has a default pool already
   (`default.<environment id>.<region>`), which is enough for the class.

2. Each student needs to be allowed to run Flink statements in the environment, and to
   read and write their topics and schemas. The role for the Flink part is
   `FlinkDeveloper` on the environment (`confluent iam rbac role-binding create
   --principal User:<STUDENT_USER_ID> --role FlinkDeveloper --environment <ENVIRONMENT_ID>`).
   `prepare_common_lab.sh` gives the same role to the shared service account. Statements run
   with the identity of the student; the students' own roles were not tested (the lab was
   prepared with an organization administrator).

   If statements must run as the shared **service account** (`--service-account`), `FlinkDeveloper` is
   not enough. A service account ran all statements of Exercises 1 to 8 successfully (each statement created with `--service-account`, bounded reads with `confluent flink query --service-account`) with exactly these roles. The first two rows were each missing in a failed test; the Schema Registry role was already in place and was not removed to test it:

   | Role | Scope | Why |
   |---|---|---|
   | `FlinkDeveloper` | the environment | Submit and run Flink statements. |
   | `DeveloperRead` and `DeveloperWrite` | the topics of the Kafka cluster (RBAC) | Flink finds the cluster as a database and reads and writes its topics through RBAC. Kafka ACLs are not enough: without these two roles a statement fails with "A database with name or id ... cannot be resolved". |
   | `ResourceOwner` | the Schema Registry subjects | Create the schemas of new tables (`prepare_common_lab.sh` grants it). |

3. Tell the students the environment ID, the Kafka cluster ID and the compute pool ID.

## Running the statements

Run every SQL statement of the exercises in one of two ways. The statements are the same.

**The Flink shell (interactive).** Start it once and type the statements there, ending each
with `;`:

```bash
confluent flink shell --compute-pool <COMPUTE_POOL_ID> --environment <ENVIRONMENT_ID> --database <KAFKA_CLUSTER_ID>
```

The shell was not tested interactively when the lab was prepared; every statement was
run with the two commands below, which use the same service.
  
**Single commands (scriptable).** A bounded `SELECT` and the results:

```bash
confluent flink query --compute-pool <COMPUTE_POOL_ID> --environment <ENVIRONMENT_ID> \
  --database <KAFKA_CLUSTER_ID> --sql "SELECT * FROM pageviews_<STUDENT_ID>;"
```

Any statement that must keep running or finish by itself (`INSERT`, `CREATE TABLE ...
AS SELECT`) with a name, which must be unique in the environment (lowercase letters,
digits and `-`). `confluent flink query` stops its statement when the query returns, so
do not use it for `INSERT` or `CREATE TABLE ... AS SELECT`:

```bash
confluent flink statement create load-pageviews-<STUDENT_ID> --compute-pool <COMPUTE_POOL_ID> \
  --environment <ENVIRONMENT_ID> --database <KAFKA_CLUSTER_ID> --wait \
  --sql "INSERT INTO ..."
```

Things to know about the pace of Flink on Confluent Cloud:

- A statement needs 5 to 20 seconds to start, so `INSERT` and `CREATE TABLE ... AS SELECT`
  results appear after a short wait. `--wait` returns when the statement is `RUNNING`, not
  when it has finished.
- A `CREATE TABLE` (or `DROP TABLE`) statement may need a few seconds before `SHOW TABLES`
  shows the change.
- Check a statement with `confluent flink statement describe <name> --cloud aws --region
  ap-south-1 --environment <ENVIRONMENT_ID>` (`COMPLETED` for a finished one, `RUNNING` for
  a continuous one).

## Exercise 1: Tables and data

Create a table of page views and a table of users. Each `CREATE TABLE` creates a Kafka
topic with 1 bucket (partition) and registers a JSON schema.

`event_ts` is a computed column: the event time, built from the `viewtime` milliseconds.
The `WATERMARK` line says that rows may arrive up to 5 seconds late. The `users` table has a
primary key and the `upsert` changelog mode: a later row for the same `user_id` replaces
the earlier one, like a compacted topic.

```sql
CREATE TABLE pageviews_<STUDENT_ID> (
  viewtime BIGINT,
  user_id STRING NOT NULL,
  page_id STRING,
  event_ts AS TO_TIMESTAMP_LTZ(viewtime, 3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) DISTRIBUTED INTO 1 BUCKETS WITH ('value.format' = 'json-registry');

CREATE TABLE users_<STUDENT_ID> (
  user_id STRING NOT NULL,
  region STRING,
  level STRING,
  updated BIGINT,
  updated_ts AS TO_TIMESTAMP_LTZ(updated, 3),
  WATERMARK FOR updated_ts AS updated_ts - INTERVAL '5' SECOND,
  PRIMARY KEY (user_id) NOT ENFORCED
) DISTRIBUTED BY (user_id) INTO 1 BUCKETS
  WITH ('changelog.mode' = 'upsert', 'value.format' = 'json-registry', 'key.format' = 'json-registry');
```

Wait until `SHOW TABLES;` lists both tables (a `CREATE TABLE` needs a few seconds to finish;
an `INSERT` started too early fails with "table does not exist").

Load the data. `viewtime` and `updated` are epoch milliseconds. Every time is `T0` plus an
offset in seconds, where `T0 = (UNIX_TIMESTAMP() / 3600) * 3600 - 7200` is the start of the
hour two hours ago (`UNIX_TIMESTAMP()` returns seconds). `T0` is computed when each statement
runs; if the clock passes a full hour between the two statements, drop the tables as in the
Cleanup section and start again. Run each `INSERT` as one statement.

```sql
INSERT INTO users_<STUDENT_ID> VALUES
  ('u1', 'EU', 'gold', (((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) - 600) * 1000),
  ('u2', 'US', 'silver', (((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) - 600) * 1000),
  ('u3', 'APAC', 'gold', (((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) - 600) * 1000);

INSERT INTO pageviews_<STUDENT_ID> VALUES
  ((((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 0) * 1000, 'u1', 'home'),
  ((((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 10) * 1000, 'u2', 'checkout'),
  ((((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 20) * 1000, 'u1', 'checkout'),
  ((((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 30) * 1000, 'u3', 'home'),
  ((((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 70) * 1000, 'u1', 'cart'),
  ((((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 80) * 1000, 'u2', 'home'),
  ((((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 140) * 1000, 'u1', 'checkout');
```

Wait until both `INSERT` statements are `COMPLETED`, then look at the data and the tables:

```sql
SELECT user_id, page_id, event_ts FROM pageviews_<STUDENT_ID>;
SELECT * FROM users_<STUDENT_ID>;
DESCRIBE pageviews_<STUDENT_ID>;
SHOW CREATE TABLE users_<STUDENT_ID>;
```

Questions:

1. Which Kafka topic backs each table, and how many partitions does it have? (Look at the
   topic list of the cluster in the Cloud console, or `python3 manage_topics.py list`.)
2. `SHOW CREATE TABLE` shows options that you did not write. Which ones, and what do they
   mean?
3. `user_id` is declared `NOT NULL` in `pageviews_<STUDENT_ID>`. Which statement of exercise
   5 would fail with "Invalid primary key ... is nullable" if it was not? (See the
   troubleshooting table at the end.)

## Exercise 2: Bounded and continuous queries

A `SELECT` on a Kafka table is a continuous query: it keeps waiting for new rows. The
`confluent flink query` command (and the `LIMIT` clause) ends it, so you see a result. In
the shell, a `SELECT` shows the rows and goes on until you leave the result view.

```sql
SELECT user_id, page_id FROM pageviews_<STUDENT_ID> WHERE page_id = 'checkout';
SELECT page_id, COUNT(*) AS view_count FROM pageviews_<STUDENT_ID> GROUP BY page_id;
SELECT $rowtime, user_id, event_ts FROM pageviews_<STUDENT_ID>;
```

Questions:

1. The second query is a count, but `confluent flink query` shows one row per page. When
   would a continuous version of this query show more than one row per page?
2. `$rowtime` is a column of every table. How does it differ from `event_ts`?

## Exercise 3: A continuous statement (filter)

`CREATE TABLE ... AS SELECT` creates a new table (topic and schema) and starts a statement
that fills it continuously. Give the statement a name with `statement create` (in the
shell it gets a generated name; see `SHOW JOBS` or the Cloud console):

```sql
CREATE TABLE checkout_views_<STUDENT_ID> DISTRIBUTED INTO 1 BUCKETS WITH ('value.format' = 'json-registry') AS
  SELECT user_id, page_id, event_ts FROM pageviews_<STUDENT_ID> WHERE page_id = 'checkout';
```

List the statements, and read the result table:

```bash
confluent flink statement list --cloud aws --region ap-south-1 --environment <ENVIRONMENT_ID>
```

```sql
SELECT * FROM checkout_views_<STUDENT_ID>;
```

Now add one more checkout to the source table (offset 160 seconds). Wait for the
statement to complete and read `checkout_views_<STUDENT_ID>` again:

```sql
INSERT INTO pageviews_<STUDENT_ID> VALUES
  ((((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 160) * 1000, 'u3', 'checkout');
```

```sql
SELECT * FROM checkout_views_<STUDENT_ID>;
```

Questions:

1. What is the status of the `CREATE TABLE ... AS SELECT` statement, and why is it not
   `COMPLETED`?
2. Does `checkout_views_<STUDENT_ID>` get the new row without you starting anything?

## Exercise 4: A temporal join

Add the region and level of the user to every page view. A temporal join
(`FOR SYSTEM_TIME AS OF`) looks up, for each page view, the version of the user row that
was valid at the time of that page view.

```sql
CREATE TABLE pageviews_enriched_<STUDENT_ID> DISTRIBUTED INTO 1 BUCKETS WITH ('value.format' = 'json-registry') AS
  SELECT p.user_id, p.page_id, p.event_ts, u.region, u.level
  FROM pageviews_<STUDENT_ID> AS p
  LEFT JOIN users_<STUDENT_ID> FOR SYSTEM_TIME AS OF p.event_ts AS u ON p.user_id = u.user_id;
```

Wait about a minute, then read the result. Then change `u2` with a time after all existing
page views, add a page view after it, wait about a minute (the join waits for the
watermarks) and read the result again:

```sql
SELECT user_id, page_id, region, level FROM pageviews_enriched_<STUDENT_ID>;

INSERT INTO users_<STUDENT_ID> VALUES
  ('u2', 'US', 'gold', (((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 300) * 1000);
INSERT INTO pageviews_<STUDENT_ID> VALUES
  ((((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 310) * 1000, 'u2', 'cart');
```

```sql
SELECT user_id, page_id, level FROM pageviews_enriched_<STUDENT_ID> WHERE user_id = 'u2';
```

Questions:

1. Why do the old page views of `u2` still show `silver`?
2. The join waits for the watermarks of both tables. What would stop the result from
   appearing if the `users` table never got a new row? (Cloud Flink moves a watermark on
   when a source is idle; look at how long the first result took.)
3. What is different from a regular join (`JOIN ... ON` without `FOR SYSTEM_TIME AS OF`)?

## Exercise 5: Windows and a count per key

```sql
CREATE TABLE views_per_minute_<STUDENT_ID> DISTRIBUTED INTO 1 BUCKETS WITH ('value.format' = 'json-registry') AS
  SELECT window_start, window_end, page_id, COUNT(*) AS view_count
  FROM TABLE(TUMBLE(TABLE pageviews_<STUDENT_ID>, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, page_id;

CREATE TABLE views_per_user_<STUDENT_ID> DISTRIBUTED INTO 1 BUCKETS WITH ('value.format' = 'json-registry') AS
  SELECT user_id, COUNT(*) AS view_count FROM pageviews_<STUDENT_ID> GROUP BY user_id;
```

Wait about a minute, then read both:

```sql
SELECT page_id, window_start, view_count FROM views_per_minute_<STUDENT_ID>;
SELECT * FROM views_per_user_<STUDENT_ID>;
```

Look at the windows in the result. The page view of exercise 4 (`u2`, `cart`, offset 310
seconds, in minute 5) has no window yet: a window is written when the watermark passes its
end, and no event after minute 5 has arrived. Add a page view in a later minute (offset 400
seconds, minute 6), wait about a minute, and read again:

```sql
INSERT INTO pageviews_<STUDENT_ID> VALUES
  ((((UNIX_TIMESTAMP() / 3600) * 3600 - 7200) + 400) * 1000, 'u3', 'home');
```

```sql
SELECT page_id, window_start, view_count FROM views_per_minute_<STUDENT_ID>;
SELECT * FROM views_per_user_<STUDENT_ID>;
```

Questions:

1. How many rows does each window of `views_per_minute` have, and why is the window of
   minute 5 missing until you added the event of offset 400? (Think of the watermark.)
2. `views_per_user` shows one row per user, but the statement writes more rows to its topic.
   Why? Which changelog mode does the table use? (`SHOW CREATE TABLE views_per_user_<STUDENT_ID>;`)
3. Why is the result of the window count written once per window, while the result of the
   count per user is updated with every event?

## Exercise 6: Avro and Schema Registry

The tables so far are JSON. Write a copy as Avro. Flink registers the schema in the Schema
Registry of the environment:

```sql
CREATE TABLE pageviews_avro_<STUDENT_ID> DISTRIBUTED INTO 1 BUCKETS WITH ('value.format' = 'avro-registry') AS
  SELECT user_id, page_id, event_ts FROM pageviews_<STUDENT_ID>;
```

Wait for the statement, read the table, then look at the schemas that Flink registered for
all your tables:

```sql
SELECT * FROM pageviews_avro_<STUDENT_ID>;
```

```bash
cd ./confluent-cloud
python3 - <<'PY'
import client_config
from confluent_kafka.schema_registry import SchemaRegistryClient

registry = SchemaRegistryClient(client_config.schema_registry_config())
for subject in sorted(s for s in registry.get_subjects() if "_<STUDENT_ID>" in s):
    version = registry.get_latest_version(subject)
    print(subject, version.schema.schema_type)
version = registry.get_latest_version("pageviews_avro_<STUDENT_ID>-value")
print(version.schema.schema_str)
PY
```

Questions:

1. Which subjects exist, and which have a `-key` subject? Which format is each?
2. What are the field types of the Avro schema? How was the `TIMESTAMP_LTZ` column
   written?
3. A table with a primary key writes a key subject. Which of your tables has one?

## Exercise 7: Inspect the statements

```sql
EXPLAIN SELECT user_id, COUNT(*) AS view_count FROM pageviews_<STUDENT_ID> GROUP BY user_id;
```

```bash
confluent flink statement list --cloud aws --region ap-south-1 --environment <ENVIRONMENT_ID>
confluent flink statement describe <STATEMENT_NAME> --cloud aws --region ap-south-1 --environment <ENVIRONMENT_ID>
confluent flink statement exception list <STATEMENT_NAME> --cloud aws --region ap-south-1 --environment <ENVIRONMENT_ID>
```

Use the name of one of your continuous statements (generated names start with `cli-` in
the shell and with `cli-query-` for `confluent flink query`). In the Cloud console, the
Flink page of the environment also shows the statements, their status and the CFUs they use.

Questions:

1. How does the plan of the count differ from the plan of a filter such as
   `SELECT user_id FROM pageviews_<STUDENT_ID> WHERE page_id = 'checkout';`? Which operators
   appear only in the count? (`confluent flink query` plans a bounded query, so the
   operator names start with `Batch`.)
2. What does `exception list` show for a statement that works? Create a statement with a
   wrong table name (`confluent flink statement create <NAME> --sql "SELECT * FROM
   no_such_table;" ...`): where do you find the reason that it failed, `describe` or
   `exception list`? (Delete that statement afterwards.)
3. Which statements of yours are still `RUNNING`, and what do they cost while they run?

## Exercise 8: A long-running job under the service account

A job that must keep running after you log out, or that belongs to the class and not to a
person, runs as a **service account**. Add `--service-account` when you create the statement.
The statement runs in the compute pool in Confluent Cloud with the identity and the roles of the
service account, not yours; nothing runs on your computer. The roles that the service account
needs are in "Instructor setup".

Create two tables (as in Exercise 1) and some orders. Wait a few seconds between the
statements, so that `SHOW TABLES` lists a new table before you use it (see "Running the
statements"):

```sql
CREATE TABLE orders_<STUDENT_ID> (order_id STRING NOT NULL, customer_id STRING NOT NULL, amount DOUBLE)
  DISTRIBUTED INTO 1 BUCKETS WITH ('value.format' = 'json-registry');

CREATE TABLE totals_<STUDENT_ID> (customer_id STRING NOT NULL, order_count BIGINT, total_amount DOUBLE,
  PRIMARY KEY (customer_id) NOT ENFORCED) DISTRIBUTED BY (customer_id) INTO 1 BUCKETS
  WITH ('changelog.mode' = 'upsert', 'value.format' = 'json-registry', 'key.format' = 'json-registry');

INSERT INTO orders_<STUDENT_ID> VALUES ('o1', 'c1', 10.0), ('o2', 'c2', 5.0), ('o3', 'c1', 2.5);
```

Submit the long-running job as the service account (`<SERVICE_ACCOUNT_ID>` is given by the
instructor):

```bash
confluent flink statement create totals-job-<STUDENT_ID> --compute-pool <COMPUTE_POOL_ID> \
  --environment <ENVIRONMENT_ID> --database <DATABASE_NAME e.g. cluster_0> --service-account <SERVICE_ACCOUNT_ID> --wait \
  --sql "INSERT INTO totals_<STUDENT_ID> SELECT customer_id, COUNT(*), SUM(amount) FROM orders_<STUDENT_ID> GROUP BY customer_id;"
```

Describe it. `Status` is `RUNNING`, `Compute Pool` is your pool and `Principal` is the service
account:

```bash
confluent flink statement describe totals-job-<STUDENT_ID> --cloud aws --region ap-south-1 --environment <ENVIRONMENT_ID>
```

Read the totals with a bounded read (it reads from the beginning to the end of the topic and
ends). The job needs up to a minute after it is `RUNNING` before the first rows appear:

```bash
confluent flink query --compute-pool <COMPUTE_POOL_ID> --environment <ENVIRONMENT_ID> --database <KAFKA_CLUSTER_ID> \
  --sql "SELECT * FROM totals_<STUDENT_ID> /*+ OPTIONS('scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset') */;"
```

Expected: `c1` with 2 orders and 12.5, `c2` with 1 order and 5.0.

Now insert one more order (`INSERT INTO orders_<STUDENT_ID> VALUES ('o4', 'c2', 20.0);`, as a
`statement create` with a name) and read the totals again: `c2` has 2 orders and 25.0 without
anything restarted. The job keeps running in the cloud.

**Stop and resume.** A stopped statement keeps its definition and its position in the topic and
does not use CFUs:

```bash
confluent flink statement stop totals-job-<STUDENT_ID> --cloud aws --region ap-south-1 --environment <ENVIRONMENT_ID>
confluent flink statement resume totals-job-<STUDENT_ID> --cloud aws --region ap-south-1 --environment <ENVIRONMENT_ID>
```

Stop the job, insert another order, and read the totals: they do not change. Resume the job:
it continues from where it stopped and catches up, and no order is counted twice. After a
resume the job needed about 1 to 3 minutes before the new order showed up in the totals.

Questions:

1. Which identity runs the statement with `--service-account`, and which identity needs the
   topic roles? What would the error be if the service account had no `DeveloperRead`?
2. Why does the job still run when you close your terminal or log out of the CLI?
3. Why is it cheaper to stop a job than to leave it running while no orders arrive?

The same statements can be submitted from a Python program (the PyFlink package
`confluent-pyflink`, with a Flink API key of the service account), for example from an
application; the job runs in the same way, in the compute pool.

## Cleanup

Delete your continuous statements (the ones that are `RUNNING`: the `CREATE TABLE ... AS
SELECT` ones and `totals-job-<STUDENT_ID>`), so that they stop using CFUs. Do not delete the statements of the other
students. List yours first:

```bash
confluent flink statement list --cloud aws --region ap-south-1 --environment <ENVIRONMENT_ID> -o json
```

```bash
confluent flink statement delete <STATEMENT_NAME> --cloud aws --region ap-south-1 --environment <ENVIRONMENT_ID> --force
```

Then drop your tables. A `DROP TABLE` deletes the Kafka topic with all its data, and the
Schema Registry subjects:

```sql
DROP TABLE totals_<STUDENT_ID>;
DROP TABLE orders_<STUDENT_ID>;
DROP TABLE pageviews_avro_<STUDENT_ID>;
DROP TABLE views_per_user_<STUDENT_ID>;
DROP TABLE views_per_minute_<STUDENT_ID>;
DROP TABLE pageviews_enriched_<STUDENT_ID>;
DROP TABLE checkout_views_<STUDENT_ID>;
DROP TABLE users_<STUDENT_ID>;
DROP TABLE pageviews_<STUDENT_ID>;
SHOW TABLES;
```

A `DROP TABLE` is a statement too, and it can take up to a minute: do not delete it before it is
`COMPLETED`, or the table stays. Delete the statements first, as above. Dropping a table does not stop the statement that
writes to it: in testing, such statements stayed `RUNNING` after their table was dropped.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `failed to initialize console parser` | The shell needs a real terminal; run it in an interactive terminal, not behind a pipe. |
| `Flink compute pool ... not found in aws ap-east-1` | `confluent flink statement` commands use the region of the CLI. Add `--cloud aws --region ap-south-1` (the region of the compute pool). |
| `The query returned no rows ... RUNNING` | The statement was started and returned before the result existed. Wait and run the query again, or look at the statement status. |
| A table is missing in `SHOW TABLES` right after `CREATE TABLE` | The statement is not finished yet; wait a few seconds. |
| `INSERT` with `confluent flink query` writes nothing | `confluent flink query` stops its statement when it returns. Use `confluent flink statement create ... --wait`. |
| A windowed query misses the last window | The watermark did not pass the end of the window; a later event closes it (exercise 5). |
| `SQL parse failed` on a column named `views` | `views` is a reserved word. Use another name such as `view_count`. |
| `Invalid primary key ... Column 'user_id' is nullable` | A primary key (or the key of a count) must be `NOT NULL`. Declare the column `NOT NULL` (exercise 1). |
| `... already exists` | A table or statement with that name exists, from another student or an earlier run of yours. Use your own `<STUDENT_ID>`, or drop and delete it. |
