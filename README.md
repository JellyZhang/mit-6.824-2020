# mit-6.824-2020

Go implements of mit-6.824 labs.

- [x] Lab1



## Lab1

### Run

```bash
git clone https://github.com/JellyZhang/mit-6.824-2020.git
cd mit-6.824-2020
docker run -v $PWD:/go/src/6.824  golang:1.15-stretch /bin/bash -c 'cd /go/src/6.824/src/main && bash /go/src/6.824/src/main/test-mr.sh'

// or you can test many times with:
// docker run -v $PWD:/go/src/6.824  golang:1.15-stretch /bin/bash -c 'cd /go/src/6.824/src/main && bash /go/src/6.824/src/main/test-mr-many.sh 3'
```

### Pic

![image-20210320213420771](https://tva1.sinaimg.cn/large/008eGmZEly1goqp49lpjuj30w80guq7s.jpg)

### Comments

1. coordinator's duty:
   - give out an un-done task to worker when a worker asks for.
   - get notified by worker when some task is done.
   - check task is finished or not after it is given out in 10 seconds.(accroding to guide)
   - control job-period.
     - if all map tasks are done, then start giving out reduce tasks.
     - if all reduce tasks are done, then tell workers to shut down themselves.
2. worker's duty:
   - ask coordinator for a task.
     - if it is a map task, then do mapf.
     - if it is a reduce task, then do reducef.
   - notify coordinator when his task is finished.
3. Some important places:
   - use a error code or something  to pass status information through rpc
   - use sync.Mutex to lock when coordinator access all tasks.
   - Put a task back to taskQueue if its worker dont response in 10 seconds.
   
   
