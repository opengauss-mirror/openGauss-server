# Prolonged Database Shutdown Time Due to Replay State

## Symptom

Executing `cm_ctl stop` results in an excessively long shutdown duration.

## Troubleshooting Method

1.  Navigate to the `$GAUSSLOG/cm/cm_agent` directory and search for the keyword `dms_contrl.sh -clean`. Verify whether the `dms_contrl.sh` script was invoked correctly.
2.  Inspect the logs in `$GAUSSLOG/pg_log` for the string `Gaussdb exit(0)`. If this message is absent, the database did not terminate gracefully.
3.  After restarting the database, check the `$GAUSSLOG/pg_log` logs for entries containing the word `Recovery`. If present, it indicates that buffer flushing took too long, preventing `gs_ctl stop` from completing gracefully. Consequently, the `dms_contrl.sh` script forcibly killed the `gaussdb` process, forcing the node into recovery upon restart.

## Root Cause

During a normal `cm_ctl stop` operation, CM invokes the `dms_contrl.sh` script to stop the database. However, if buffer flushing takes too long, `gs_ctl stop` fails to complete within the allocated timeout. As a result, `dms_contrl.sh` forcibly terminates the database process.
