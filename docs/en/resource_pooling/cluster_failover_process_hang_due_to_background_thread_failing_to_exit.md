# Cluster Failover Process Hang Due to Background Thread Failing to Exit

## Symptom

In a primary-standby deployment running TPCC workloads without transaction limits, the cluster exhibits occasional RTO violations during failover testing. Specifically, after injecting a fault on the primary node and terminating the primary process, the observed RTO exceeds 30 seconds, occasionally reaching up to 60 seconds.

1. Monitor the cluster state and wait for the standby node to promote. Inspect the database logs: `cd $GAUSSLOG/pg_log/dn_6001/` and search globally for `failover trigger`.
2. Analysis of the database logs reveals two consecutive failover records on the promoting node, resulting in a total RTO duration of approximately two minutes, as shown below:

    ```shell
    2024-07-25 09:34:50.892 [unknown] [unknown] localhost 281453068005328 0[0:0#0] 0[DMS] LOG: [SS failover] failover trigger.
    2024-07-25 09:35:21.964 [unknown][unknown] localhost 281453068005328 0[0:0#0]0 [DMS] WARNING: [SS failover] failover failed, backends can not exit
    2024-07-25 09:35:26.336 [unknown] [unknown] localhost 281450146672592 0[0:0#0]0 [DMS] LOG: [SS failover] failover trigger.
    2024-07-25 09:35:26.352 [unknown] [unknown] localhost 281450146672592 0[0:0#0]0 [DMS] LOG: [SS failover] do failover when DB restart.
    2024-07-25 09:37:17.4 [postmaster][reaper][281459746799632] LOG: database system is ready to accept connections
    ```

## Troubleshooting Method

1. Inspect the database logs. Following the initial `failover trigger`, the system initiates a `failover reform` process. Between the first trigger log and the `failover failed` message, numerous entries appear indicating connection failures due to the postmaster state. The following logs illustrate this behavior:

    ```shell
    3013 2024-07-25 15:19:59.991 [unknown] [unknown] localhost 281446460463264 0[0:0#0] o [DMS] LOG: [ss failover] failover trigger.a
    3014 2024-07-25 15:19:59.991 [unknown] [unknown] localhost 281446460463264 0[0:0#0] o [DMS] LOG: [SS reform] dms reform start, role:1, reform type:failover reform
    3015 2024-07-25 15:20:00.563 65040562.5175 postgres 281445888989344 cm_agent 0 dn_6001_6002 42809 4222124650660924 [BACKEND] ERROR: failed to request snapshot as current node is in reform!
    3016 2024-07-25 15:20:00.563 65040562.5175 postgres 281445888989344 cm_agent 0 dn_60016002 42809 4222124650660924 [BACKEND] STATEMENT: select local role,static_connections,db_state,detail information from pg_stat _get stream replications();
    3017 2024-07-25 15:20:01.59865040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3018 2024-07-25 15:20:02.613 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_60016002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3019 apons dweqreqeas qpssneb/up/e:ep/lle1suT/ssneßuado/Jaqsewbo/awoy/, alTJ ouks) :qpssne6 :907 [ON3NOV8] 0 00000 Z009 T009 up 0 [uMouxun] 8ZT88T9ZO/SVT8Z [UMouyun] T ZESOVOS9 £VZ.80:07:ST ST-60-87OZ
    ss
    3020 2024-07-25 15:20:03.243 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_6001_6002 00000 0 [DMS] LOG: [SS failover] kill backends begin.
    3021 2024-07-25 15:20:03.243 6504055b.5080 postgres 281449418496160 JobSchedulerO dn_6001_6002 00000 0 [BACKEND] LOG: job scheduler is shutting down
    3022 2024-07-25 15:20:03.626 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 600201000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3023 2024-07-25 15:20:04.639 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3024 2024-07-25 15:20:05.651 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001_6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT_BACKENDS
    3025 2024-07-25 15:20:06.664 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3026 2024-07-25 15:20:07.676 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3027 2024-07-25 15:20:08.688 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3028 2024-07-25 15:20:09.701 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3029 2024-07-25 15:20:10.714 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3030 2024-07-25 15:20:11.727 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3031 2024-07-25 15:20:12.740 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3032 2024-07-25 15:20:13.753 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3033 2024-07-25 15:20:14.767 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3034 2024-07-25 15:20:15.780 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_6001_6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3036 2024-07-25 15:20:17.806 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_6001_6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3037 2024-07-25 15:20:18.820 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND ] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3038 2024-07-25 15:20:19.833 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3039 2024-07-25 15:20:20.846 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3040 2024-07-25 15:20:21.859 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3041 2024-07-25 15:20:22.872 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3042 2024-07-25 15:20:23.885 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_6001_6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3043 2024-07-25 15:20:24.898 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3044 2024-07-25 15:20:25.911 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_6001_6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3045 2024-07-25 15:20:26.924 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3046 2024-07-25 15:20:27.936 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3047 2024-07-25 15:20:28.947 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3048 2024-07-25 15:20:29.959 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3049 2024-07-25 15:20:30.970 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3050 2024-07-25 15:20:31.982 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3051 2024-07-25 15:20:32.993 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0[BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3052 2024-07-25 15:20:33.410 [unknown] [unknown] localhost 281446460463264 0[0:0#0]o [DMS] WARNING: [SS failover] failover failed, backends can not exit
    ```

2. These logs indicate that the `PM_WAIT_BACKENDS` state is obstructing the failover process and preventing the reform procedure from progressing.
3. The interval between the first `failover trigger` and the subsequent database restart spans approximately two and a half minutes, encompassing two failover attempts. The root cause of the first failure is explicitly logged as `backends can not exit`.

## Root Cause

1. During the failover promotion process on the standby node while business workloads are active, the business threads (backends) fail to exit promptly. This results in the failure of the first online failover attempt. In response, CM automatically triggers a second restart-based failover to ensure successful promotion.
2. The first online failover incurs a latency of approximately 30 seconds due to the blocked backend exit process. The subsequent restart-based failover introduces additional overhead, further extending the total RTO duration.

## Solution

Two potential solutions are available to address this issue:

1. **Aggressive Exit Mechanism:** Introduce a timeout flag for backend requests during cluster reform scenarios. If the timeout threshold is exceeded, forcibly terminate all business threads to unblock the failover process.
2. **Graceful Exit Mechanism:** Implement an orderly shutdown sequence propagating upwards from the underlying business threads to ensure clean termination.

**Expected Outcome:** Ensure the success of the first failover attempt and maintain a total RTO duration of less than 30 seconds.
