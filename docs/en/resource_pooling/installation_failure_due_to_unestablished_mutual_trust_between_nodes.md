# Installation Failure Due to Unestablished Mutual Trust Between Nodes

## Symptom

During the installation process, error messages appear indicating that mutual trust-related files are missing or cannot be opened. Manual establishment of mutual trust is required.

## Troubleshooting Method

Based on the error message, inspect the relevant mutual trust files.

## Root Cause

Mutual trust between nodes has not been established. This typically occurs when the automated mutual trust setup detects existing but invalid `~/.ssh` files and subsequently skips the creation process. Manual intervention is required to establish mutual trust.

## Solution

Manually establish mutual trust for both the `root` user and the database user. There are two methods available: using the `gs_sshexkey` tool or manual configuration.

### Method 1: Using `gs_sshexkey`

1. Create a file named `hostfile` containing the IP addresses of all nodes.
2. Execute the following command under the target user (either `root` or the database user):

    ```shell
    gs_sshexkey -f hostfile
    ```

### Method 2: Manual Configuration

The following steps illustrate manual mutual trust establishment using a three-node cluster (hostnames: `plat1`, `plat2`, `plat3`) as an example.

1. **Generate the local authorization file on `plat1`:**
Generate the RSA key pair and append the public key to the `authorized_keys` file for the `root` user.

    ```shell
    ssh-keygen -t rsa
    cat .ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    ```

2. **Collect public keys from all nodes:**
Scan and append the RSA host keys of all nodes to the `known_hosts` file on `plat1`.

    ```shell
    ssh-keyscan -t rsa plat1 >> ~/.ssh/known_hosts
    ssh-keyscan -t rsa plat2 >> ~/.ssh/known_hosts
    ssh-keyscan -t rsa plat3 >> ~/.ssh/known_hosts
     ```

3. **Distribute the SSH configuration:**
Copy the `.ssh` directory from `plat1` to the other nodes.

    ```shell
    scp -r ~/.ssh plat2:~
    scp -r ~/.ssh plat3:~
    ```

4. **Verify the mutual trust setup:**
Attempt to log in to the remote nodes via SSH. If the login succeeds without a password prompt, mutual trust has been successfully established.

    ```shell
    ssh plat2
    ```
