# File Is Damaged in the XFS File System<a name="EN-US_TOPIC_0291615092"></a>

## Symptom<a name="section172487523295"></a>

When a cluster is in use, error reports such as an input/output error or the structure needs cleaning generally do not occur in the XFS file system.

## Cause Analysis<a name="section1744562618577"></a>

The XFS file system is abnormal.

## Procedure<a name="section873235710290"></a>

Try to mount or unmount the file system to check whether the problem can be solved.

If the problem recurs, refer to the file system document, such as  **xfs\_repair**, and ask the system administrator to restore the file system. After the file system is repaired, run the  **gs\_ctl build**  command to restore the damaged DNs.
