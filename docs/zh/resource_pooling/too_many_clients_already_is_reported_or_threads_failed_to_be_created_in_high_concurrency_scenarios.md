# 高并发报错“too many clients already”或无法创建线程

## 问题现象<a name="section1556012104711"></a>

高并发执行SQL，报错“sorry, too many clients already”；或报无法创建线程、无法fork进程等错误。

## 原因分析<a name="section496011141776"></a>

该类报错是由于操作系统线程资源不足引起，查看操作系统ulimit -u，如果过小（例如小于32768），则基本可以判断是操作系统限制引起的。

## 处理办法<a name="section18593181816710"></a>

通过“ulimit -u”命令查看操作系统max user processes的值。

```
[root@openGauss36 mnt]# ulimit -u
unlimited
```

按如下简易公式计算需要设置的最小值。

```
value=max(32768，实例数目*8192)
```

其中实例数目指本节点所有实例总数。

设置最小值方法为，修改/etc/security/limits.conf，追加如下两行：

```
* hard nproc [value]  
* soft nproc [value] 
```

对于不同操作系统修改方式略有不同，centos6以上版本可以修改/etc/security/limits.d/90-nofile.conf文件，方法同上。

另外，也可以直接通过如下命令设置，但OS重启会失效，可以添加到全局环境变量/etc/profile文件中使其生效。

```
ulimit -u [values]
```

在大并发模式下，建议开启线程池，使数据库内部的线程资源受控。
