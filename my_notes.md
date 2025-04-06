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