# Core Fault Locating<a name="EN-US_TOPIC_0289900556"></a>

## Core Dump Occurs due to Full Disk Space<a name="EN-US_TOPIC_0289900936"></a>

### Symptom<a name="en-us_topic_0283137100_en-us_topic_0059778167_s7a2ed06fefd0448fae90f40fe4291f8d"></a>

When TPC-C is running, the disk space is full during injection. As a result, a core dump occurs on the GaussDB process, as shown in the following figure.

![](figures/en-us_image_0289900420.png)

### Cause Analysis<a name="en-us_topic_0283137100_en-us_topic_0059778167_s74d2dfcb815b4d8ca504c549a923e5ed"></a>

When the disk is full, Xlog logs cannot be written. The program exits through the panic log.

### Procedure<a name="en-us_topic_0283137100_section485620163250"></a>

Externally monitor the disk usage and periodically clean up the disk.

## Core Dump Occurs Due to Incorrect Settings of GUC Parameter log\_directory<a name="EN-US_TOPIC_0289901017"></a>

### Symptom<a name="en-us_topic_0283137178_en-us_topic_0059778167_s7a2ed06fefd0448fae90f40fe4291f8d"></a>

After the database process is started, a core dump occurs and no log is recorded.

### Cause Analysis<a name="en-us_topic_0283137178_en-us_topic_0059778167_s74d2dfcb815b4d8ca504c549a923e5ed"></a>

The directory specified by GUC parameter  **log\_directory**  cannot be read or you do not have permissions to access this directory. As a result, the verification fails during the database startup, and the program exits through the panic log.

### Procedure<a name="en-us_topic_0283137178_section485620163250"></a>

Set  **log\_directory**  to a valid directory. For details, see  [log\_directory](https://docs.opengauss.org/en/docs/latest/database_reference/logging_destination.html#en-us_topic_0283136719_en-us_topic_0237124721_en-us_topic_0059778787_sfbedf09fcf1a4223a4538679f80f12a9).

## Core Dump Occurs when RemoveIPC Is Enabled<a name="EN-US_TOPIC_0289900135"></a>

### Symptom<a name="en-us_topic_0283136554_section54529241124"></a>

The  **RemoveIPC**  parameter in the OS configuration is set to  **yes**. The database breaks down during running, and the following log information is displayed:

```
FATAL: semctl(1463124609, 3, SETVAL, 0) failed: Invalid argument
```

### Cause Analysis<a name="en-us_topic_0283136554_section444545621213"></a>

If  **RemoveIPC**  is set to  **yes**, the OS deletes the IPC resources \(shared memory and semaphore\) when the corresponding user exits. As a result, the IPC resources used by the openGauss server are cleared, causing the database to break down.

### Procedure<a name="en-us_topic_0283136554_section10754612151312"></a>

Set  **RemoveIPC**  to  **no**. For details, see  **Preparing for Installation**  \>  **Preparing the Software and Hardware Installation Environment**  \>  **Modifying OS Configuration**  in the  _Installation Guide_.
