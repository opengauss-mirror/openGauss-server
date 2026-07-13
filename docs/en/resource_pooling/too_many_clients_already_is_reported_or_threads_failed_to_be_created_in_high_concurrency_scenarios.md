# "too many clients already" Is Reported or Threads Failed To Be Created in High Concurrency Scenarios<a name="EN-US_TOPIC_0291615108"></a>

## Symptom<a name="section1556012104711"></a>

When a large number of SQL statements are concurrently executed, the error message "sorry, too many clients already" is displayed or an error is reported, indicating that threads cannot be created or processes cannot be forked.

## Cause Analysis<a name="section496011141776"></a>

These errors are caused by insufficient OS threads. Check  **ulimit -u**  in the OS. If the value is too small \(for example, less than 32768\), the errors are caused by the OS limitation.

## Procedure<a name="section18593181816710"></a>

Run  **ulimit -u**  to obtain the value of  **max user processes**  in the OS.

```
[root@openGauss36 mnt]# ulimit -uunlimited
```

Use the following formula to calculate the minimum value:

```
value=max (32768, number of instances x 8192)
```

The number of instances refers to the total number of instances on the node.

To set the minimum value, add the following two lines to the  **/etc/security/limits.conf**  file:

```
* hard nproc [value]  
* soft nproc [value] 
```

The file to be modified varies based on the OS. For versions later than CentOS6, modify the  **/etc/security/limits.d/90-nofile.conf**  file in the same way.

Alternatively, you can run the following command to change the value. However, the change becomes invalid upon OS restart. To solve this problem, you can add  **ulimit -u**\[_value_\] to the global environment variable file  **/etc/profile**.

```
ulimit -u [values]
```

In high concurrency mode, enable the thread pool to control thread resources in the database.
