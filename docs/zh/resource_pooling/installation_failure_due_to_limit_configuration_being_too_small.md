# 因limit配置过小导致安装失败的问题

## 一、问题现象

节点启动失败，或OM安装到最后一步发现集群无法启动，且发现om_monitor，cm_server，cm_agent进程都不存在。

## 二、定位方法

1. 首先检查om_monitor，cm_server，cm_agent这些进程是否存在，进入$GAUSSLOG/cm查看有没有生成新的cm日志，一般来说进程不存在应该是没有生成。

2. 在$GAUSSLOG/cm查看om_monitor日志观察是否存在类似于‘limit值小于640000’的错误提示，则说明是limit值设置过小导致的无法启动。

## 三、根因分析

1. limits.conf 文件是 Linux 系统中的一个配置文件，主要用于设置用户和用户组的资源限制。它通常位于`/etc/security/limits.conf` 路径下。limits.conf 文件的基本格式如下：

    ```shell
    <domain> <type> <item> <value>

    <domain>：可以是用户名、用户组名（以 @ 开头）或 *（表示所有用户）。
    <type>：可以是 hard（硬限制，用户无法超过的限制）或 soft（软限制，用户可以在一定范围内调整的限制）。
    <item>：要限制的资源类型，例如 nproc（进程数）、nofile（打开的文件数）、memlock（锁定的内存）等。
    <value>：限制的具体值
    ```

    以下是一个示例配置：

    ```
    1.限制用户 alice 的最大进程数为 100
    alice hard nproc 100

    2.限制所有用户的最大打开文件数为 1024
    * soft nofile 1024
    * hard nofile 2048

    3.限制用户组 staff 的最大内存使用为 1GB
    @staff hard as 1048576
    ```

## 四、解决方案

根据上述描述合理配置`/etc/security/limits.conf` 文件。

    注意：修改 limits.conf 文件后，通常需要重新登录或重启相关服务才能生效，可以重新尝试拉起集群或者重新执行安装。
