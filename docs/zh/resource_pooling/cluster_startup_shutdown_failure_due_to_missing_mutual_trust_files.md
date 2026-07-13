# 因互信文件丢失导致集群启停失败的问题

## 一、问题现象

1. 在集群停止或启动阶段，出现如下现象：

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

2. 部分节点启动或停止失败，报错出现`Exited with error code 255`，`the ssh connection time out or the ssh trust relationship is abnormal on some node`字样。

## 二、定位方法

1. 首先确认'error code 255'是由于ssh连接异常所导致的，尝试通过ssh命令连接其他节点，检查节点连接情况。

2. `cat ~/.ssh/authorized_keys` 检查互信文件是否存在，该文件包含允许访问该用户的公钥。

## 三、问题根因

CM启停集群的过程中需要，通过一些ssh命令下发其命令，如果互信文件有丢失或者有损坏可能会导致集群无法启动，同时可能会引起一些其他的问题。

## 四、解决方案

1. 生成新的 SSH 密钥对（如果没有现有的密钥对），如果没有现有的 SSH 密钥对，可以在本地机器上生成一个新的密钥对：`ssh-keygen -t rsa -b 2048`。

2. 查看公钥内容：`cat ~/.ssh/id_rsa.pub`。

3. 将公钥复制到目标主机：`ssh-copy-id user@target_host`，将 user 替换为目标主机的用户名，将 target_host 替换为目标主机的 IP 地址或主机名，系统会提示您输入目标主机的密码。

4. 如果 `ssh-copy-id` 命令不可用，您可以手动将公钥添加到目标主机的 authorized_keys 文件中。首先，SSH 登录到目标主机：`ssh user@target_host`，然后在目标主机上执行以下命令： `echo "your_public_key_content" >> ~/.ssh/authorized_keys`。

5. 设置正确的权限：`chmod 700 ~/.ssh` ，`chmod 600 ~/.ssh/authorized_keys`。

6. 对集群做一些简单的启停进行检查。
