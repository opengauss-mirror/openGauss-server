# VACUUM FULL一张表后，表文件大小无变化

## 问题现象<a name="section6396133915314"></a>

使用VACUUM FULL命令对一张表进行清理，清理完成后表大小和清理前一样大。

## 原因分析<a name="section19352134312532"></a>

假定该表的名称为table\_name，对于该现象可能有以下两种原因：

- table\_name表本身没有delete过数据，使用VACUUM FULL table\_name后无需清理delete的数据。因此表大小清理前后一样大。

- 在执行VACUUM FULL table\_name时有并发的事务存在，可能会导致VACUUM FULL跳过清理最近删除的数据，导致清理不完全。

## 处理办法<a name="section34071447115313"></a>

对于第二种可能原因，有如下两种处理方法：

- 如果在VACUUM FULL时有并发的事务存在，此时需要等待所有事务结束，再次执行VACUUM FULL命令对该表进行清理。

- 如果使用上面的方法清理后，表文件大小仍然无变化，确认无业务操作后，使用以下SQL查询活跃事务列表状态：

    ```
    select txid_current();
    ```

    使用该SQL可以查询当前的事务XID。再使用以下命令查看活跃事务列表：

    ```
    select txid_current_snapshot();
    ```

    如果发现活跃事务列表中有XID比当前的事务XID小时，停止数据库再启动数据库，再次使用VACUUM FULL命令对该表进行清理。
