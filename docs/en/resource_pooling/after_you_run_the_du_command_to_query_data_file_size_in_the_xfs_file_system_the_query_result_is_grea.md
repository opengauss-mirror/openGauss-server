# After You Run the du Command to Query Data File Size In the XFS File System, the Query Result Is Greater than the Actual File Size<a name="EN-US_TOPIC_0291615091"></a>

## Symptom<a name="section1585924603013"></a>

After you run the  **du**  command to query data file size in the cluster, the query result is probably greater than the actual file size.

```
 du -sh file
```

## Cause Analysis<a name="section1896295016309"></a>

The XFS file system has a pre-assignment mechanism. The file size is determined by the  **allocsize**  parameter. The file size displayed by the  **du**  command includes the pre-assigned disk space.

## Procedure<a name="section16959125763016"></a>

- Select the default value \(64 KB\) for the XFS file system mount parameter allocsize to eliminate the problem.

- Add the  **--apparent-size**  parameter when using the  **du**  command to query the actual file size.

```
du -sh file --apparent-size
```

- If the XFS file system reclaims the pre-assigned space of a file, the  **du**  command displays the actual file size.
