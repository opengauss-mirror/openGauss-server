# 执行 SQL 语句时，提示 Lock wait timeout

## 问题现象<a name="section158125414577"></a>

执行SQL语句时，提示“Lock wait timeout”。

```
ERROR:  Lock wait timeout: thread 140533638080272 waiting for ShareLock on relation 16409 of database 13218 after 1200000.122 ms ERROR:  Lock wait timeout: thread 140533638080272 waiting for AccessExclusiveLock on relation 16409 of database 13218 after 1200000.193 ms
```

## 原因分析<a name="section7762348125715"></a>

数据库中存在锁等待超时现象。

## 处理办法<a name="section72471253195718"></a>

- 需要分析锁超时的原因，查看系统表pg\_locks，pg\_stat\_activity可以找出超时的SQL语句。
