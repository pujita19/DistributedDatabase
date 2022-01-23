# DistributedDatabase
To distribute our data onto multiple servers, **Horizontal Fragmentation** is used which is driven by **hashing**. There is one client and 3 servers. 

The client sends a request to the leader server which then forwards it to all the worker servers with the help of multithreading. The worker executes the SQL query it received from the leader and returns the response. The leader gets the response from the workers, merges it and sends it to the client. Blocking queues are used for storing the request and response of each worker. The leader inserts the query into the relevant blocking queues and gets the response from them. Each worker has a thread dedicated to it. 

![Image depicting the specified architecture](/assets/images/image2.png)

The use of blocking queues allowes us to have only n-threads(if n- workers are being used, for eg: in our case only 3 threads are used for communication between leader and workers) rather than creating n-threads every time a client makes a SQL query request. Insert queries are sent to only one dedicated worker while all other queries are sent to all the workers. 
