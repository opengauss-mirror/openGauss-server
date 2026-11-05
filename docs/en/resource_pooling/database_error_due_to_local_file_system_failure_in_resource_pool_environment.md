# Database Error due to Local File System Failures in Resource Pool Environments

## Symptom

When operating a resource pool cluster, there is a very low probability of encountering file system errors such as `Input/Output error, structure needs cleaning` in `XFS/EXT4` systems.

## Troubleshooting Method

Check the OS logs using `dmesg`. You may find the following error message:

```
Input/Output error
```

## Root Cause

Bad sectors have developed on the disk, or the file system has become corrupted.

## Solution

First, attempt to unmount and remount the affected file system to see if this resolves the issue. If the problem persists, contact your system administrator. They should follow the official documentation for the specific file system to perform repairs and replace the disk promptly.