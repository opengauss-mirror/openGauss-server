# SET<a name="ZH-CN_TOPIC_0289899950"></a>

## 功能描述<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s8a5c6264f78f49e3aa93f388d68cd3e6"></a>

用于修改运行时配置参数。

## 注意事项<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s8cb7444b58764d99913a4cc61f397f9f"></a>

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。
- 新增支持set guc参数时to关键词成可选语法。

## 语法格式<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s29888afda1844d6f9fc677f1b59b5b7d"></a>

```
SET {config_parameter} {value};
```

## 示例<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s51d29fa208274032a4e5308b57638421"></a>

```
--设置ANSI_NULLS参数。    
test_d=# set ANSI_NULLS  to on;
SET
test_d=# set ANSI_NULLS  to off;
SET
test_d=# set ANSI_NULLS on;
SET
test_d=# set ANSI_NULLS off;
SET
```
