# SET<a name="EN_TOPIC_0289899950"></a>

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:34:58.195Z pushedAt=2026-09-22T07:17:33.732Z -->

## Description<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s8a5c6264f78f49e3aa93f388d68cd3e6"></a>

Used to modify runtime configuration parameters.

## Notes<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s8cb7444b58764d99913a4cc61f397f9f"></a>

- This section contains only the syntax newly added by shark. The original openGauss syntax has not been removed or modified.
- The TO keyword is now optional when setting GUC parameters.

## Syntax<a name="en_topic_0283136841_en_topic_0237122186_en_topic_0059779029_s29888afda1844d6f9fc677f1b59b5b7d"></a>

```
SET {config_parameter} {value};
```

## Examples<a name="en_topic_0283136841_en_topic_0237122186_en_topic_0059779029_s51d29fa208274032a4e5308b57638421"></a>

```
--Set the ANSI_NULLS parameter.
test_d=# set ANSI_NULLS  to on;
SET
test_d=# set ANSI_NULLS  to off;
SET
test_d=# set ANSI_NULLS on;
SET
test_d=# set ANSI_NULLS off;
SET
```