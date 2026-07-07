# 指定子查询不展开的Hint

## 功能描述<a name="section290819468377"></a>

数据库在对查询进行逻辑优化时通常会将可以提升的子查询提升到上层来避免嵌套执行，但对于某些本身选择率较低且可以使用索引过滤访问页面的子查询，嵌套执行不会导致性能下降过多，而提升之后扩大了查询路径的搜索范围，可能导致性能变差。对于此类情况，可以使用no\_expand Hint进行调试。大多数情况下不建议使用此hint。

## 语法格式<a name="section530131664410"></a>

```
no_expand
```

## 示例<a name="section175581239572"></a>

正常的查询执行

```
explain select * from t1 where t1.a in (select t2.a from t2);
```

计划

![](figures/zh-cn_image_0000001209615959.png)

加入no\_expand

```
explain select * from t1 where t1.a in (select /*+ no_expand*/ t2.a from t2);
```

计划

![](figures/zh-cn_image_0000001209736009.png)
