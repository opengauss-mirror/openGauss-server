# Pre-installation Failure Due to Automated Mutual Trust Setup Failure

## Symptom

During the database pre-installation phase, the automated mutual trust setup fails, resulting in a pre-installation failure.

## Troubleshooting Method

Mutual trust issues are typically diagnosed by inspecting the error messages and verifying the relevant SSH configuration files.

## Root Cause

During the pre-installation phase, `gs_preinstall` detects that the `~/.ssh` directory already exists. However, the authentication artifacts (keys or `authorized_keys`) within this directory may have expired or become invalid, preventing the script from establishing new trust relationships and causing the pre-installation to abort.

## Solution

Remove the existing `~/.ssh` directory on all affected nodes, then re-execute the pre-installation script.
