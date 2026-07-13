# 备机处于need repair\(WAL\)状态问题

## 问题现象<a name="section19264812163110"></a>

openGauss备机出现Standby Need repair\(WAL\)故障。

## 原因分析<a name="section31031614204014"></a>

因网络故障、磁盘满等原因造成主备实例连接断开，主备日志不同步，导致数据库在启动时异常。

## 处理分析<a name="section12618818144413"></a>

通过gs\_ctl build -D 命令对故障节点进行重建，具体的操作方法请参见《工具与命令参考》中“系统内部命令 \> gs_ctl”的build参数。
