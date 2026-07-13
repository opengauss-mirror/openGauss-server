# 因节点间互信未建立导致安装失败问题

## 一、问题现象

安装过程中出现缺少/打不开互信相关文件的报错信息，需要手动建立互信。

## 二、定位方法

根据报错信息，检查相关互信文件。

## 三、问题根因

节点间互信未建立，可能由于自动化互信检测到已有失效的`~/.ssh`文件于是跳过创建互信导致，需要手动建立互信。

## 四、解决方法

手工建立root用户和普通用户的互信，有两种建立互信的方式：`gs_sshexkey`建立互信和手工建立互信。

1. `gs_sshexkey`建立互信。

    a. 创建文件hostfile，在该文件中写入所有节点ip。

    b. 在需要创建互信的用户（root或普通用户）下执行以下命令。

    ```shell
    gs_sshexkey -f hostfile
    ```

2. 手工建立互信。

    以三节点集群为例说明手工建立互信过程，plat1、plat2、plat3是主机名。

    a. 在集群中的某一主机plat1上生成root用户的本机授权文件`authorized_keys`。

    ```shell
    ssh-keygen -t rsa
    cat .ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    ```

    b. 在plat1上收集所有待建立互信主机的公钥，写入plat1的`kown_hosts`文件中。

    ```shell
    ssh-keyscan -t rsa plat1 >> ~/.ssh/known_hosts
    ssh-keyscan -t rsa plat2 >> ~/.ssh/known_hosts
    ssh-keyscan -t rsa plat3 >> ~/.ssh/known_hosts
     ```

    c. 将互信文件分发到其他主机上。

    ```shell
    scp -r ~/.ssh plat2:~
    scp -r ~/.ssh plat3:~
    ```

    d. 检查互信是否建立成功，通过ssh命令能够进入对应主机，则建立成功。

    ```shell
    ssh plat2
    ```
