# When the TPC-C is running and a disk to be injected is full, the TPC-C stops responding.<a name="EN-US_TOPIC_0289900262"></a>

## Symptom<a name="en-us_topic_0283136782_section526616331423"></a>

When the TPC-C is running and a disk to be injected is full, the TPC-C stops responding. After the fault is rectified, the TPC-C automatically continues to run.

## Cause Analysis<a name="en-us_topic_0283136782_section1710704115427"></a>

When the disk where the performance log \(**gs\_profile**\) is located is full, the database cannot write data and enters the infinite waiting state. As a result, the TPC-C stops responding. After the disk space insufficiency fault is rectified, performance logs can be properly written, and the TPC-C is restored.

## Procedure<a name="en-us_topic_0283136782_section12323144814214"></a>

Externally monitor the disk usage and periodically clean up the disk.
