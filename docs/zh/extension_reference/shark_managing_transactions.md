# 管理事务

事务是用户定义的一个数据库操作序列，这些操作要么全做要么全不做，是一个不可分割的工作单位。shark插件支持的事务控制命令有启动、设置保存点、提交、回滚事务。

## 注意事项

- 本章节只包含shark新增的语法，原openGauss的事务控制语法由于无法避免的冲突仅删除begin transaction transaction_mode语法，冲突的语法可使用类似语法begin transaction_mode或者start transaction transaction_mode代替。
- D库的PL/pgSQL不支持BEGIN TRAN和BEGIN TRANSACTION语法。

## 语法格式

- 启动事务

    使用BEGIN语法启动事务。transaction_name没有实际意义，仅语法兼容。

    ```
    BEGIN { TRAN | TRANSACTION } [ { transaction_name } ];
    ```

- 设置保存点

    使用save语法为当前数据状态做一个标记，它允许将那些在它建立后执行的命令全部回滚，把事务的状态恢复到保存点所在的时刻。

    设置保存点。

    ```
    SAVE { TRAN | TRANSACTION } { savepoint_name };
    ```

    回滚保存点。

    ```
    ROLLBACK { TRAN | TRANSACTION } { savepoint_name };
    ```

- 提交事务

    使用COMMIT完成提交事务的功能，即提交事务的所有操作。transaction_name没有实际意义，仅语法兼容。

    ```
    COMMIT [ { TRAN | TRANSACTION } [ transaction_name ] ];
    ```

- 回滚事务

    使用ROLLBACK回滚到某个保存点或回滚事务中的所有操作。带有savepoint_name参数时回滚到对应保存点，否则回滚所有操作。

    ```
    ROLLBACK { TRAN | TRANSACTION } [ savepoint_name ];
    ```

## 参数说明

- **TRAN | TRANSACTION**

    BEGIN格式中的可选关键字，没有实际作用。

- **transaction_name**

    无实际意义，仅语法兼容。

- **SAVE**

    设置保存点。

- **savepoint_name**

    保存点名称，后续使用ROLLBACK命令时可以用。

- **COMMIT**

    提交当前事务，让所有当前事务的更改为其他事务可见。

- **ROLLBACK**

    回滚当前事务，撤销操作。

## 示例

```
opengauss=# create table t1 (c1 int);
CREATE TABLE
opengauss=# begin tran;
BEGIN
opengauss=# insert into t1 values(1);
INSERT 0 1
opengauss=# save tran savepoint1;
SAVEPOINT
opengauss=# insert into t1 values(2);
INSERT 0 1
opengauss=# rollback tran savepoint1;
ROLLBACK
opengauss=# commit tran;
COMMIT
opengauss=# select * from t1;
 c1 
----
  1
(1 row)

```
