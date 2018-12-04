Training a model usually can take several hours or days, even using a large number of machines, a long-running job is likely to experience failures. Fault tolerance is a critical feature for the distributed system, when the error occurs, the system should continue working and make sure the training quality been afftected as small as possible.

Checkpoint was a wide used technique in parameter server framework to guarantee fault toleance, it is particular suitable for cluster up to 100s of machines, system like Bosen, Petuum, Husky and FlexPS have implement and adopt this strategy.

Checkpoint make sure the stateful components in the system dump the model parameter and configuration data to the persistent storage like HDFS at fixed time interval(e.g. every 5 minutes), or at fixed numbers of iterations based on users' chioce. Every servers and works need send their heartbeats to the scheduler at fixed time, if the failure was detected, the system will stop and restart the task with model parameter reloading and progress recovery.

Comparsion bettwen different fault toleance techniques:

|Name|Strategy|System|
|---|---|---|
|Checkpoint|Save data and configuration in the checkpoint, restart and reload data when error occurs|Tensorflow, Bosen, Petuum, FlexPs|
|Data Replication|Live replication of parameters, support hot failover|HDFS, Ps-lite|

In the project, checkpoint is more suitable than data replication as it need less overhead. A brief schedule for implement this feature:

|Period|Process|
|---|---|
|Nov. 2 - 9|complete the code implementation of checkpoint|
|Nov. 10 - 16|make a fault toleranec application and pass the test|
|Nov. 17 - 30|Try other features such as elastic scalability and load balancing if time allowed|







