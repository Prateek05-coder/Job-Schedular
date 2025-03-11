# Job-Schedular
Job Scheduler design and implementation in C++
# C++ Program to Simulate Job Scheduler for in a Supercomputer.
 Suppose the supercomputer has one master scheduler node and 128 worker nodes. All
 the worker nodes have 24 cores and 64 GBs of RAM. All the jobs from the users get
 queued up at the master node and the master node schedule the jobs on to the worker
 node. Each job is specified with arrival time, the number of cores required to execute the
 job, amount of memory in GB require to execute the job, and execution time (in the
 number of hours) of the job.
 The master node accumulates all the jobs reached to master node at beginning of current
 hours and all the pending older jobs (which are in the queue but not yet allocated to any
 workers) and try to allocate worker node for each of the jobs one after another in order.
 Worker node allocation for a job is successful if the master node finds a worker node with
 a higher amount of available resources than the required resource by the job. Otherwise,
 the job gets reinserted into the queue.
 Master node use three policies to put the Jobs into the queue :
 (a) FCFS (b) smallest job first queue, (c) short duration job first queue.
 Short duration job means a job with a smaller execution time and the smallest job means a
 job with the smallest gross value (=execution time * CPU required * Memory required).
 Master node use three policies to find a worker node for the job and these policies are (a)
 first fit, (b) best fit, and (c) worse fit.
 Simulate the job scheduler for the above queuing policies and node selection policies.
 Generate average CPU, Memory utilization per day basis for each combination of policies,
and show the same in graphical format (putting the same in a CSV file and general bar
 graphs).
 Assume each job behaves ideally as specified, which means each job consumes the
 specified amount of resources (not less and not more) and takes the specified amount of
 time to execute (not less and not more).
 
 Data Set:
 https://drive.google.com/file/d/1nk2vwcl0BHpmQDdOXVh2iBSaA63kcVQl/view?usp=sharing
