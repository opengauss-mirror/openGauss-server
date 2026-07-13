# 因hostname文件权限异常导致设置参数失败的问题

## 一、 问题现象

资源池化环境安装成功后，出现部分节点参数设置失败信息：

``` shell
    Total nodes: 3. Failed nodes: 2.
    Failed node names:
        [openGaussxxx]
        [openGaussxxx]
    ALL: Failure to perform gs_guc!
```

同时，如果在该用户下使用`ssh`通过`host name`连接其他节点，直接报错失败。

## 二、 定位方法

上述问题的主要原因为`host`权限异常、或文件丢失，进而导致使用、依赖`ssh`指令的工具失效，因此需要到`/etc`目录下检查`hostname`文件是否存在，以及权限是否正常，预期为：

```shell
    -rw-r--r--. 1 root root  13 Nov 22 15:33 /etc/hostname
```

如果该文件不存在，或者权限异常，需要进行重建和授权。

## 三、 问题根因

因`gs_guc`工具依赖`ssh`工具进行参数设置，需要保证`ssh`功能可用，且互信建立成功。当`hostname`文件不存在或者权限异常时，用户无法使用互信连接目标主机，进而多节点设置参数失败。

## 四、 解决方案  

出现该问题，首先需要检查`hostname`是否存在，或权限是否异常。确定无误后，清楚三节点的数据库用户目录下的`~/.ssh`互信信息，使用安装包`scripts`目录下的`gs_sshexkey`工具重新进行互信建立。
