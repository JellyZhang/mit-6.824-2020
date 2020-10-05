# mit-6.824
Go implements of mit-6.824 labs.





## Lab1

1. master's duty:
   - give out an un-done task to worker when a worker asks for.
   - get noticed by worker when some task is done.
   - check task is finished or not after it is given out in 10 seconds.
   - control job-period.
     - if all map tasks are done, then start giving out reduce tasks.
     - if all reduce tasks are done, then tell workers to shut down themselves.
2. worker's duty:
   - ask master for a task.
     - if it is a map task, then do mapf.
     - if it is a reduce task, then do reducef.
   - notice master when his task is finished.

3. Some important places:
   - use a flag to pass status information through rpc, like Errcode.
   - use sync.Mutex to lock when master access all tasks.
4. test pic:

![image-20201005184828042](https://tva1.sinaimg.cn/large/007S8ZIlly1gjenghneflj31700ks471.jpg)

- The `dialing` message is a worker trying to call a closed master, then it will shutdown himself.
- This is not out-of-control.
- Test Environment: Debian 9,  go 1.15 linux/amd64, GO111MODULE=off.