# Pre-installation or Installation Failure Due to Missing Software Dependencies (e.g., glibc)

## Symptom

The pre-installation or installation process fails due to a missing software dependency. For example, if `libreadline.so.7` is absent, the following error message appears:

```shell
error while loading shared libraries: libreadline.so.7: cannot open shared object file: No such file or directory
```

## Troubleshooting Method

Inspect the error message to identify the missing library, then verify whether the corresponding software package is installed on the operating system.

## Root Cause

The operating system lacks one or more software packages required for the openGauss installation.

## Solution

Install the missing software packages. To ensure a successful cluster installation, the following dependencies must be present on the OS: `glibc`, `libaio-devel`, `libnsl` (required specifically for **openEuler + x86** environments), and `libreadline.so.7`
