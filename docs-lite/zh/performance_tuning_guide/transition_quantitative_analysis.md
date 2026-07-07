# 行存转向量化<a name="ZH-CN_TOPIC_0000001265287497"></a>

## 可获得性<a name="section15406143204715"></a>

本特性自openGauss 3.0.0版本开始引入。

## 特性简介<a name="section740615433477"></a>

将行存表的查询转换为向量化执行计划执行，提升复杂查询的执行性能。

## 客户价值<a name="section13406743164715"></a>

由于行存执行引擎在执行包含较多表达式或者关联操作的复杂查询时，性能表现不佳；而向量化执行引擎在执行复杂查询时具有优异的性能表现。所以通过将行存表的查询转换为向量化执行计划执行，能够有效提升复杂查询的查询性能。

## 特性描述<a name="section16406154310471"></a>

本特性通过对扫描算子增加一层RowToVec的操作，将行存表的数据在内存中变为向量化格式后，上层算子都能够转化为对应的向量化算子，从而使用向量化执行引擎计算。支持行转列的扫描算子包括：SeqScan、IndexOnlyscan、IndexScan、BitmapScan、FunctionScan、ValueScan、TidScan。

## 特性增强<a name="section1340684315478"></a>

无。

## 特性约束<a name="section06531946143616"></a>

- 不支持向量化的场景包括：
    - targetList存在返回set的函数。
    - targetList或者qual中存在不支持向量化的表达式：数组类表达式计算；多子查询表达式计算；Field类表达式计算；系统表列。
    - 包含不支持向量化的类型：POINTOID；LSEGOID；BOXOID；LINEOID；CIRCLEOID；POLYGONOID；PATHOID；用户自定义类型。

- MOT表不支持转向量化。
