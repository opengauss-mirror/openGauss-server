# Case Study: Tuning Startup Parameters to Fix TPCC Fluctuations

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-17T06:49:15.224Z -->

## Symptom Description

On a standalone 4-socket Kunpeng server running openGauss, TPCC results sit around 2 million most of the time, but occasionally spike above 2.3 million about once a week. The fluctuation is significant.

The openGauss startup command in use:

```shell
# $datadir is the database node path.
numactl -C 1-28,32-60,64-92,96-124,128-156,160-188,192-220,224-252 gs_ctl start -D $datadir  -Z single_node
```

## Analysis

On a 4‑socket Kunpeng server, CPU is more powerful, but xlog flush speed becomes the bottleneck. The cost of cross‑NUMA memory access looks like this:

```shell
node distances:
node   0   1   2   3   4   5   6   7
  0:  10  11  24  25  24  25  24  25
  1:  11  10  25  32  25  32  25  32
  2:  24  25  10  11  24  25  24  25
  3:  25  32  11  10  25  32  25  32
  4:  24  25  24  25  10  11  24  25
  5:  25  32  25  32  11  10  25  32
  6:  24  25  24  25  24  25  10  11
  7:  25  32  25  32  25  32  11  10

```

As you can see, memory access costs vary a lot across NUMA nodes. So pinning related operations to the same NUMA node can significantly speed things up.


In this scenario, xlog is the bottleneck. The disk hosting xlog is mounted on NUMA node 0. So if we allocate xlog‑related memory on node 0 first, xlog write speed improves noticeably.


To check which NUMA node an NVMe disk belongs to:


```shell
# nvme0 is the actual disk identifier number.
cat /sys/class/nvme/nvme0/device/numa_node
```

The optimized database startup command is as follows:

```shell
numactl -C 1-28,32-60,64-92,96-124,128-156,160-188,192-220,224-252 --preferred=0 gs_ctl restart -D $datadir  -Z single_node
```

The `--preferred=0` option tells the system to allocate xlog memory on NUMA node 0 first. This fixes the TPCC fluctuation issue.
