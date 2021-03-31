# mit-6.824-2021

Go implements of mit-6.824 labs.

# Table of Contents

- [mit-6.824-2021](#mit-6824-2021)
  - [Table of Contents](#table-of-contents)
    - [Lab1](#lab1)
      - [Run](#run)
      - [Pic](#pic)
      - [Comments](#comments)
    - [Lab2A](#lab2a)
      - [Run](#run-1)
      - [Pic](#pic-1)
      - [Comments](#comments-1)
    - [Lab2B](#lab2b)
      - [Run](#run-2)
      - [Pic](#pic-2)
      - [Comments](#comments-2)

## Lab1

### Run

```bash
git clone https://github.com/JellyZhang/mit-6.824-2021.git
cd mit-6.824-2021
docker run -v $PWD:/6.824 -w /6.824/src/main  golang:1.15-stretch bash ./test-mr.sh

// or you can test many times with:
// docker run -v $PWD:/6.824 -w /6.824/src/main  golang:1.15-stretch bash ./test-mr-many.sh 3
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
   - use a error code or something to pass status information through rpc
   - use sync.Mutex to lock when coordinator access all tasks.
   - Put a task back to taskQueue if its worker dont response in 10 seconds.

## Lab2A

### Run

```shell
git clone https://github.com/JellyZhang/mit-6.824-2021.git
cd mit-6.824-2021
docker run -v $PWD:/6.824 -w /6.824/src/raft golang:1.15-stretch go test -race -run 2A
```

### Pic

![image-20210324214432091](https://tva1.sinaimg.cn/large/008eGmZEly1govbw2sog4j32780fkh1t.jpg)

### Comments

- [raft Visualizations](http://thesecretlivesofdata.com/raft/)

- There are two important tickers:

  - election ticker: decide whether to start an election or not. (Follower or Candidate)
    - get node's last timestamp of receving leader's heartbeat.
    - sleep for a random time.(election timeout). (according to guide, it should be greater than 150~300ms).
    - get node's last timestamp of receving leader's heartbeat again, check if it has changed.
    - if two timestamps are same, which means there is no heartbeat during sleeping, then start an election.
  - Leader ticker: send heartbeats to Followers and Candidates periodically.
    - accoding to guide, heartbeats duration should be greater than 100ms.

- Some tips:
  - Use goroutines to start RequestVote RPC call and AppendEnties RPC call makes code easy debugging.
  - When gathering votes, we can use sync.Cond to wait for enough votes.
  - Use mutex to lock when reading or writing.
  - It is Important to record a copy of node's original situation before sleeping, because term/role may change during sleep.

## Lab2B

### Run

```shell
git clone https://github.com/JellyZhang/mit-6.824-2021.git
cd mit-6.824-2021
docker run -v $PWD:/6.824 -w /6.824/src/raft golang:1.15-stretch go test -race -run 2B
```

### Pic

![image-20210331223739999](https://tva1.sinaimg.cn/large/008eGmZEly1gp3grjmhxsj30rr0fidj7.jpg)

### Comments

- Lab2B is more difficult than Lab1 and Lab2A, better read the paper and watch some instruments first.

  - [raft paper](http://nil.csail.mit.edu/6.824/2020/papers/raft-extended.pdf)

- some important tips:
  - **Figure 2 is very very important, you can basically just copy every variable name and find out every code logic from it. Once you implement Figure 2, you have done Lab2B**
  - Basic Process:
    1. Leader append logs to Followers.
    2. Leader find out that one LogEntry (assume index=`X` ) have appended in majority of the cluster.
    3. Leader increase his commitIndex to `X`, and apply commands before `X` by applyChannel.
    4. Leader tell Followers that commitIndex is `X` now in next AppendEntries RpcCall.
    5. Follower find out that LeaderCommitIndex has increased, set his commitIndex to `X`, and apply commands before `X`
  - Follower only give vote to Candidate if his log is more up-to-date. You should read the 5.4.1 to find out how to define "up-to-date".
  - Follower only append logs if leader's _prevLogIndex_ and _prevLogTerm_ match Follower's.
  - ApplyChannal is used for one node to apply command to tester when _commitIndex_ increases.
  - If you finished Lab2B but find out it takes about 60s to finish TestBackUp2B, you should optimize the way you decrease nextIndex[i]. see more in the quoted section in raft paper 5.3.
