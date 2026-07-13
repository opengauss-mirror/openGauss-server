# 业务运行时整数转换错

## 问题现象<a name="section24920212218"></a>

在转换整数时报错如下。

```
Invalid input syntax for integer: "13."
```

## 原因分析<a name="section1071410284216"></a>

部分数据类型不能转换成目标数据类型。

## 处理办法<a name="section2142103410214"></a>

逐步缩小SQL范围确定不能转换的数据类型。
