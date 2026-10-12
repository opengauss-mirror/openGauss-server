# Installation Failure Due to Abnormal Disk Ownership

## Symptom

The database installation fails with the following error message:

```shell
[GAUSS-50315]: The user xxx is not matched with the owner of /dev/sda.
```

## Troubleshooting Method

Use the `ls` command to verify whether the disk owner matches the current user. If it does not, use the `chown` command to change the ownership to the current user.

```shell
ls -l /dev/sda
brw-rw---- 1 root disk 8, 1 Oct  5 10:27 /dev/sda
```

## Root Cause

The database generally does not modify disk ownership. This issue is typically caused by accidental operations performed by administrators or other users, or by modifications made by other programs.

## Solution

Use the `chown` command to change the disk owner to the current user. For example, to change the owner of disk `/dev/sda` to user `xxx`, run the following command:

```shell
chown xxx /dev/sda
```
