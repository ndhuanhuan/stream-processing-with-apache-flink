# Ch1
To start the project, run
```
docker compose up
```

Then create 5 topics
```
sh redpanda-setup.sh
```

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

