# Cluster Startup and Shutdown Failure Due to Missing Mutual Trust Files

## Symptom

1. During the cluster stop or start phase, the following phenomena occur:

    ```shell
    cm_ctl: stop cluster. 
    cm_ctl: stop nodeid: 1
    cm_ctl: stop nodeid: 2
    cm_ctl: stop nodeid: 3
    [1] 10:55:06 [FAILURE] NodeIP_2 Exited with error code 255
    [1] 10:55:10 [SUCCESS] NodeIP_3
    [1] 10:55:10 [SUCCESS] NodeIP_1
    ....
    cm_ctl: abnormal node 2.
    ..
    cm_ctl: abnormal node 2.
    .....................
    cm_ctl: stop cluster partly successfully.



    cm_ctl: checking cluster status.
    cm_ctl: checking cluster status.
    cm_ctl: checking finished in 1253 ms.
    cm_ctl: The ssh connection time out or the ssh trust relationship is abnormal on some nodes. But the cluster will continue to start.
    cm_ctl: start cluster. 
    cm_ctl: start nodeid: 1
    cm_ctl: start nodeid: 2
    cm_ctl: start nodeid: 3
    [1] 10:55:39 [FAILURE] NodeIP_2 Exited with error code 255
    [1] 10:55:40 [SUCCESS] NodeIP_3
    [1] 10:55:40 [SUCCESS] NodeIP_1
    ..........
    ```

2. Some nodes fail to start or stop, and the error messages contain `Exited with error code 255` and `the ssh connection time out or the ssh trust relationship is abnormal on some node`.

## Troubleshooting Method

1. First, confirm that `error code 255` is caused by an SSH connection anomaly. Attempt to connect to other nodes using the `ssh` command to verify connectivity.

2. Execute `cat ~/.ssh/authorized_keys` to check whether the mutual trust file exists. This file contains the public keys authorized to access the user account.

## Root Cause

During cluster startup and shutdown, CM relies on SSH commands to dispatch instructions to remote nodes. If the mutual trust files are missing or corrupted, the cluster may fail to start or stop, and this condition may also trigger additional issues.

## Solution

1. Generate a new SSH key pair (if none exists). If the local machine does not have an existing SSH key pair, generate one using the following command:

    ```bash
    ssh-keygen -t rsa -b 2048
    ```

2. Display the public key content:

    ```bash
    cat ~/.ssh/id_rsa.pub
    ```

3. Copy the public key to the target host:

    ```bash
    ssh-copy-id user@target_host
    ```

    Replace `user` with the username on the target host and `target_host` with the target host's IP address or hostname. You will be prompted to enter the password for the target host.

4. If the `ssh-copy-id` command is not available, manually append the public key to the `authorized_keys` file on the target host. First, SSH into the target host:

    ```bash
    ssh user@target_host
    ```

    Then, execute the following command on the target host:

    ```bash
    echo "your_public_key_content" >> ~/.ssh/authorized_keys
    ```

5. Set the correct permissions:

    ```bash
    chmod 700 ~/.ssh
    chmod 600 ~/.ssh/authorized_keys
    ```

6. Perform simple start and stop operations on the cluster to verify that the mutual trust relationship has been restored.
