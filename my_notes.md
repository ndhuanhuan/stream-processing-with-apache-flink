# Ch1
To start the project, run
```
docker compose up
```

To clean the data
```
docker compose down -v
```

Then create 5 topics
```
sh redpanda-setup.sh
```

Flink UI: http://localhost:8081/#/overview
Redpanda UI: http://localhost:8080/

The last thing we need is data on those topics.

I have provided the following producers - TransactionsProducer.java and StateProducer.java.

Run the two producers and you should see the data ingested within the topics.

```
src/main/java/io/streamingledger/producers/TransactionsProducer.java
src/main/java/io/streamingledger/producers/StateProducer.java
```

## Flink basics
The JobManager is the master process and then you have TaskManagers that act as worker nodes.

The JobManager consists of a dispatcher. The dispatcher has a rest endpoint that is used for job submission. It also launches the Flink Web UI and spawns a JobMaster for each Job.

A JobMaster performs the scheduling of the application tasks on the available TaskManager worker nodes. It also acts as a checkpoint coordinator.

We also have the Resource Manager. When TaskManagers start, they register themselves with a Resource Manager and offer available slots. A slot is where the application tasks are executed and define the number of tasks a TaskManager can execute.

So on a high level, a Flink cluster consists of one (or more) JobManager which is the master process, and TaskManagers that act as workers.

While your Flink cluster is up and running, you can navigate to the Flink UI at http://localhost:8081/#/overview.

When the JobManager receives a JobGraph:
- Converts the JobGraph into the ExecutionGraph
- Requests resources; TaskManager slots to execute the tasks
- When it receives the slots it schedules the task on those slots for execution.
- During execution, it acts as a coordinator for required actions like checkpoint coordination

Task Chaining is a way of merging two or more operators together that reduces the overhead of local communication.
When two or more operators are chained together they can be executed by the same task.
There are two prerequisites for the chaining to be applied:
- The operators need to have the same level of parallelism
- The operators must be connected with a forward data exchange strategy.

Other data exchange strategies can include:
- Broadcast strategy: Upstream outputs sent as input to all downstream operators
- Random Strategy: Upstream outputs send randomly to downstream operators
- Key-Based Strategy: Upstream outputs send to downstream operators according on some partition key.

## TaskManager sizing
Typically you should try having medium-sized TaskManagers.
Putting your cluster to the test should give you a rough estimate of the proper size.
Your cluster needs to have enough hardware resources available to each TaskManager and you should also find a good number of slots per TaskManager.
You can also set the configuration cluster.evenly-spread-out-slots to true to spread out the slots evenly among the TaskManagers.
TaskManagers are JVM processes so having quite large TaskManagers that perform heavy processing can result in performance issues also due to Garbage Collection running.
Other things to consider that might harm performance:
- Setting the parallelism for each operator (overriding job and cluster defaults)
- Disabling operator chaining
- Using slot-sharing groups to force operators into their own slots

## Slot Sharing
By default, subtasks will share slots, as long as:
- they are from the same job
- they are not instances of the same operator chain

Thus one slot may hold an entire pipeline number of slots = max available parallelism.
Slot sharing leads to better resource utilization, by putting lightweight and heavyweight tasks together.
But in rare cases, it can be useful to force one or more operators into their own slots.
Remember that a slot may run many tasks/threads.
Typically you might need one or two CPUs per slot.
Use more CPUs per slot if each slot (each parallel instance of the job) will perform many CPU-intensive operations.

## Deployment Modes
### Mini Cluster
A mini-cluster is a single JVM process with a client, a JobManager and TaskManagers. It is a good and convenient choice for running tests and debugging in your IDE locally.

### Session Cluster
A session cluster is a long-lived cluster and it’s lifetime is not bound to the lifetime of any Flink Job. You can run multiple jobs, but there is no isolation between jobs - TaskManagers are shared. This has the downside that if one TaskManager crashes, then all jobs that have tasks running on this TaskManager will fail. It is well-suited though for cases that you need to run many short-lived jobs like ad-hoc SQL queries for example.

### Application Cluster
An application cluster only executes a job from one Flink application. The main method runs on the cluster, not on the client and the application jar along with the required dependencies (including Flink itself) can be pre-uploaded.
This allows you to deploy a Flink application like any other application on Kubernetes easily and since the ResourceManager and Dispatcher are scoped within a single Flink Application it provides good resource isolation.

# CH2 - Streams and Tables
## Streaming SQL Semantics
## Dynamic table
These events yield changes, which result in the output table being continuously updated.
This is called a Dynamic Table

## Flink SQL Logical Components
Flink consists of catalogs that hold metadata for databases, tables, functions, and views.
A catalog can be non-persisted (In-memory catalog) or persistent backed by an external system like the PostgresCatalog, the PulsarCatalog, and the HiveCatalog.
For In-memory catalogs, all metadata will be available only for the lifetime of the session.
In contrast, catalogs like the PostgresCatalog enables users to connect the two systems and then Flink automatically references existing metadata by mapping them to its corresponding metadata.

Within the catalogs, you create databases and tables within the databases.
When creating a table its full table name identifier is: <catalog_name>.<database_name>.<table_name> and when a catalog and/or database is not specified the default ones are used.

Make sure you produced some data
```
io.streamingledger.producers.TransactionsProducer
io.streamingledger.producers.StateProducer
```

Run command:
```
docker exec -it jobmanager ./bin/sql-client.sh
```

Some examples:
```
SHOW CATALOGS;
CREATE DATABASE bank;
SHOW DATABASES;
USE bank;
SHOW TABLES;
SHOW VIEWS;
SHOW FUNCTIONS;


SET sql-client.execution.result-mode = tableau;

CREATE TABLE transactions (
    transactionId      STRING,
    accountId          STRING,
    customerId         STRING,
    eventTime          BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),
    eventTimeFormatted STRING,
    type               STRING,
    operation          STRING,
    amount             DOUBLE,
    balance            DOUBLE,
        WATERMARK FOR eventTime_ltz AS eventTime_ltz
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'redpanda:9092', --//'kafka:29092' <-- for kafka use this
    'properties.group.id' = 'group.transactions',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

SELECT
    transactionId,
    eventTime_ltz,
    type,
    amount,
    balance
FROM transactions;

CREATE TABLE customers (
    customerId STRING,
    sex STRING,
    social STRING,
    fullName STRING,
    phone STRING,
    email STRING,
    address1 STRING,
    address2 STRING,
    city STRING,
    state STRING,
    zipcode STRING,
    districtId STRING,
    birthDate STRING,
    updateTime BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
        WATERMARK FOR eventTime_ltz AS eventTime_ltz,
            PRIMARY KEY (customerId) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'customers',
    'properties.bootstrap.servers' = 'redpanda:9092', --//'kafka:29092' <-- for kafka use this
    'key.format' = 'raw',
    'value.format' = 'json',
    'properties.group.id' = 'group.customers'
);

SELECT
    customerId,
    fullName,
    social,
    birthDate,
    updateTime
FROM customers;

CREATE TABLE accounts (
    accountId STRING,
    districtId INT,
    frequency STRING,
    creationDate STRING,
    updateTime BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
        WATERMARK FOR eventTime_ltz AS eventTime_ltz,
            PRIMARY KEY (accountId) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'accounts',
    'properties.bootstrap.servers' = 'redpanda:9092', --//'kafka:29092' <-- for kafka use this
    'key.format' = 'raw',
    'value.format' = 'json',
    'properties.group.id' = 'group.accounts'
);

SELECT *
FROM accounts
LIMIT 10;


SELECT
    transactionId,
    eventTime_ltz,
    type,
    amount,
    balance
FROM transactions
WHERE amount > 180000
    and type = 'Credit';

SELECT
    transactionId,
    eventTime_ltz,
    type,
    amount,
    balance
FROM transactions
WHERE amount > 180000
    and type = 'Credit'
ORDER BY eventTime_ltz;

CREATE TEMPORARY VIEW temp_premium AS
SELECT
    transactionId,
    eventTime_ltz,
    type,
    amount,
    balance
FROM transactions
WHERE amount > 180000
  and type = 'Credit';

SELECT * FROM temp_premium;

SHOW VIEWS;

SELECT customerId, COUNT(transactionId) as txnPerCustomer
FROM transactions
GROUP BY customerId;

SELECT *
FROM (
         SELECT customerId, COUNT(transactionId) as txnPerCustomer
         FROM transactions
         GROUP BY customerId
     ) as e
WHERE txnPerCustomer > 500;

SELECT
    transactionId,
    eventTime_ltz,
    type,
    amount,
    balance
FROM transactions
ORDER BY eventTime_ltz;



SELECT
    transactionId,
    eventTime_ltz,
    convert_tz(cast(eventTime_ltz as string), 'Europe/London', 'UTC') AS eventTime_ltz_utc,
    type,
    amount,
    balance
FROM transactions
WHERE amount > 180000
  and type = 'Credit';


-- Deduplication
-- This query basically tells Flink - for every transactionsId (that should be unique) order them by their event time and assign them a number.
-- For example, in cases we have duplicates we will have two row numbers of 1 and 2 for the same transaction id, and from those two we are only interested in keeping the first one.


SELECT transactionId, rowNum
FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY transactionId ORDER BY eventTime_ltz) AS rowNum
         FROM transactions)
WHERE rowNum = 1;
```

# Chapter watermark and window
```
SET sql-client.execution.result-mode = tableau;

CREATE TABLE transactions (
    transactionId      STRING,
    accountId          STRING,
    customerId         STRING,
    eventTime          BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),
    eventTimeFormatted STRING,
    type               STRING,
    operation          STRING,
    amount             DOUBLE,
    balance            DOUBLE,
        WATERMARK FOR eventTime_ltz AS eventTime_ltz
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'group.transactions',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);


SELECT
    transactionId,
    eventTime_ltz
FROM transactions
LIMIT 10;


-- How many transactions per day?
-- This query uses a TUMBLE window to count transactions in non-overlapping 1-day intervals
-- TUMBLE creates fixed-size windows (tumbling windows) that don't overlap
-- DESCRIPTOR(eventTime_ltz) specifies the event time column for windowing
-- window_start and window_end are automatically generated to show each window's time boundaries
-- Each window produces one result row showing the count of transactions for that day
SELECT
    window_start AS windowStart,
    window_end as windowEnd,
    COUNT(transactionId) as txnCount
FROM TABLE(
        TUMBLE(TABLE transactions, DESCRIPTOR(eventTime_ltz), INTERVAL '1' DAY)
    )
GROUP BY window_start, window_end;


-- Sliding window -- 
SELECT
    window_start AS windowStart,
    window_end as windowEnd,
    COUNT(transactionId) as txnCount
FROM TABLE(
        HOP(TABLE transactions, DESCRIPTOR(eventTime_ltz), INTERVAL '2' HOUR, INTERVAL '1' DAY)
    )
GROUP BY window_start, window_end;

-- A Cumulative Window is similar to a Sliding Window with the difference that the window doesn’t slide, but the starting bound stays the same until the window reaches the specified interval.
-- More specifically let’s say we want one day window, with a 2 hour interval.
The window will wait for the 1 day to pass, but will also be updating and firing results every two hours since the window start.

SELECT
    window_start AS windowStart,
    window_end as windowEnd,
    window_time AS windowTime,
    COUNT(transactionId) as txnCount
FROM TABLE(
        CUMULATE(TABLE transactions, DESCRIPTOR(eventTime_ltz), INTERVAL '2' HOUR, INTERVAL '1' DAY)
    )
GROUP BY window_start, window_end, window_time;

-- Session Window
-- Top-3 Customers per week (max # of transactions)
-- This is a Top-N pattern query that finds the 3 customers with most transactions in each 7-day week
-- Despite the "Session Window" comment, this actually uses TUMBLE window
-- 
-- How it works (3 nested queries):
-- 1. Innermost query - Aggregation:
--    - Creates 7-day tumbling windows
--    - Groups by window AND customerId
--    - Counts transactions per customer per week
-- 2. Middle query - Ranking:
--    - Assigns a rank to each customer within each window using ROW_NUMBER()
--    - PARTITION BY window_start, window_end ensures ranking restarts for each week
--    - ORDER BY txnCount DESC ranks by transaction count (highest first)
-- 3. Outermost query - Filtering:
--    - WHERE rowNum <= 3 keeps only the top 3 ranked customers per week
--
-- Result: For each 7-day period, you get the 3 customers who had the most transactions
SELECT *
FROM (
         SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY txnCount DESC) as rowNum
         FROM (
                  SELECT
                      customerId,
                      window_start,
                      window_end,
                      COUNT(transactionId) as txnCount
                  FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(eventTime_ltz), INTERVAL '7' DAY))
                  GROUP BY window_start, window_end, customerId
              )
     ) WHERE rowNum <= 3;
```
All the events flowing through Flink pipelines and being processed are considered StreamElements.
These StreamElements can be either `StreamRecords` (i.e every event that is being processed) or a `Watermark`.
A watermark is nothing more than a special record injected into the stream that carries a timestamp (t).

Flink allows you to achieve this by using a `WatermarkStrategy`.
A WatermarkStrategy informs Flink how to extract an event’s timestamp and assign watermarks.

The current watermark for a task with multiple inputs is the minimum watermark from all of its input

```
SELECT
    window_start AS windowStart,
    window_end as windowEnd,
    COUNT(transactionId) as txnCount
FROM TABLE(
        TUMBLE(TABLE transactions, DESCRIPTOR(eventTime_ltz), INTERVAL '7' DAY)
    )
GROUP BY window_start, window_end;
```

# Chapter Streaming Joins
## Regular Join
The join result is updated whenever records of either input table are inserted, deleted, or updated
Regular joins work well if both input tables are not growing too large.
If the tables start to grow too large you need to account for that and have a strategy to expire it.

```
SET sql-client.execution.result-mode = tableau;

CREATE TABLE transactions (
    transactionId      STRING,
    accountId          STRING,
    customerId         STRING,
    eventTime          BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),
    eventTimeFormatted STRING,
    type               STRING,
    operation          STRING,
    amount             DOUBLE,
    balance            DOUBLE,
        WATERMARK FOR eventTime_ltz AS eventTime_ltz
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'group.transactions',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE customers (
    customerId STRING,
    sex STRING,
    social STRING,
    fullName STRING,
    phone STRING,
    email STRING,
    address1 STRING,
    address2 STRING,
    city STRING,
    state STRING,
    zipcode STRING,
    districtId STRING,
    birthDate STRING,
    updateTime BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
        WATERMARK FOR eventTime_ltz AS eventTime_ltz,
            PRIMARY KEY (customerId) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'customers',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'key.format' = 'raw',
    'value.format' = 'json',
    'properties.group.id' = 'group.customers'
);

CREATE TEMPORARY VIEW txnWithCustomerInfo AS
SELECT
    transactionId,
    t.eventTime_ltz,
    type,
    amount,
    balance,
    fullName,
    email,
    address1
FROM transactions AS t
         JOIN customers AS c
              ON t.customerId = c.customerId;

CREATE TEMPORARY VIEW txnWithCustomerInfoDedup AS
 SELECT *
 FROM (
          SELECT *,
                 ROW_NUMBER() OVER (PARTITION BY transactionId ORDER BY eventTime_ltz) AS rowNum
          FROM txnWithCustomerInfo)
WHERE rowNum = 1;
```

## Interval Joins
An interval join allows joining records of two append-only tables such that the time attributes of joined records are not more than a specified window interval apart. Tables must be append-only - rows can’t be updated.
```
CREATE TABLE debits (
    transactionId      STRING,
    accountId          STRING,
    customerId         STRING,
    eventTime          BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),
    eventTimeFormatted STRING,
    type               STRING,
    operation          STRING,
    amount             DOUBLE,
    balance            DOUBLE,
        WATERMARK FOR eventTime_ltz AS eventTime_ltz
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions.debits',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'group.transactions.debits',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);


CREATE TABLE credits (
    transactionId      STRING,
    accountId          STRING,
    customerId         STRING,
    eventTime          BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),
    eventTimeFormatted STRING,
    type               STRING,
    operation          STRING,
    amount             DOUBLE,
    balance            DOUBLE,
        WATERMARK FOR eventTime_ltz AS eventTime_ltz
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions.credits',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'group.transactions.credits',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

SELECT
    d.transactionId AS debitId,
    d.customerId    AS debitCid,
    d.eventTime_ltz AS debitEventTime,
    c.transactionId AS creditId,
    c.customerId    AS creditCid,
    c.eventTime_ltz AS creditEventTime
FROM debits d, credits c
WHERE d.customerId = c.customerId
  AND d.eventTime_ltz BETWEEN c.eventTime_ltz - INTERVAL '1' HOUR AND c.eventTime_ltz;
```

## Temporal Joins
A temporal table gives access to its history.
Each record of an append-only table is joined with the version of the temporal table that corresponds to that record’s timestamp.
Temporal tables give access to versions of an append-only history table:
Must have a primary key, and
Updates that are versioned by a time attribute
Requires an equality predicate on the primary key
As time passes, versions no longer needed are removed from the state.
Temporal joins are extremely useful we using streaming storage layers as the source, like Redpanda, Apache Kafka, or Pulsar.
A common streaming use case is having an append-only stream and also other changelog streams.
Changelog streams are backed by compacted topics - typically used for storing some kind of state.
You are only interested in the latest value per key, in order to implement use cases like data enrichment
```
SET 'table.exec.source.idle-timeout'='100';
SELECT
    transactionId,
    t.eventTime_ltz,
    TO_TIMESTAMP_LTZ(updateTime, 3) as updateTime,
    type,
    amount,
    balance,
    fullName,
    email,
    address1
FROM transactions AS t
    JOIN customers FOR SYSTEM_TIME AS OF t.eventTime_ltz AS c
ON t.customerId = c.customerId;
```
It’s similar to a regular join, with the difference that you use the FOR SYSTEM_TIME AS OF syntax in order to create versions.

## Lookup Joins
```
SELECT
    transactionId,
    t.accountId,
    t.eventTime_ltz,
    TO_TIMESTAMP_LTZ(updateTime, 3) AS updateTime,
    type,
    amount,
    balance,
    districtId,
    frequency
FROM transactions AS t
         JOIN accounts FOR SYSTEM_TIME AS OF t.eventTime_ltz AS a
              ON t.accountId = a.accountId;
```

# Chapter UDF
Fix macbook java env first:
```
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

Go to root of project and run
```
mvn clean package
```

```
docker compose up

docker exec -it jobmanager bash
```



To deploy a jar
```
docker exec -it jobmanager ./bin/flink run \
--class io.streamingledger.datastream.BufferingStream \
jars/spf-0.1.0.jar
```

This will package your application and you should see a spf-0.1.0.jar file under the target folder.

Copy the file into the jars folder along with the connector jars folder located at the root of your project so that is is included it into our flink image.

Note: You can delete the previous images to make sure the images are created with all the jars in place.

If you run docker compose up you should see your containers running.

If you open a terminal in the JobManager container `docker exec -it jobmanager bash` you should see spf-0.1.0.jar inside the jars folder.

### Register UDF
```shell
CREATE FUNCTION maskfn  AS 'io.streamingledger.udfs.MaskingFn'      LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';
CREATE FUNCTION splitfn AS 'io.streamingledger.udfs.SplitFn'        LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';
CREATE FUNCTION lookup  AS 'io.streamingledger.udfs.AsyncLookupFn'  LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';

CREATE TEMPORARY VIEW sample AS
SELECT * 
FROM transactions 
LIMIT 10;

SELECT transactionId, maskfn(UUID()) AS maskedCN FROM transactions;
SELECT * FROM transactions, LATERAL TABLE(splitfn(operation));

SELECT 
  transactionId,
  serviceResponse, 
  responseTime 
FROM sample, LATERAL TABLE(lookup(transactionId));
```

# Chapter The DataStream API