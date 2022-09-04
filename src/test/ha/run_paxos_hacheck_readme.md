## The method of running paxos hacheck
```bash
cd src/test/ha
# The following parameter 1 is loop times
# and 16001 is the dcf port of the first datanode
sh run_paxos_single.sh 1 16001
```