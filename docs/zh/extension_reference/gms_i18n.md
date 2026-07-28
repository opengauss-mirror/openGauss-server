# gms_i18n

## gms_i18n概述

gms_i18n是一个基于openGauss的插件，提供国际化功能。目前支持的函数有：GMS_I18N.RAW_TO_CHAR、GMS_I18N.STRING_TO_RAW。

## gms_i18n限制

- 仅支持Create extension命令方式加载插件。

## gms_i18n安装

openGauss打包编译时默认已经包含了gms_i18n, 可以在安装完openGauss后，直接通过create extension gms_i18n;加载插件。

## gms_i18n使用

### 创建Extension<a name="section21088306113"></a>

创建gms_i18n Extension可直接使用CREATE Extension命令进行创建：

```
openGauss=# CREATE Extension gms_i18n;
```

### 使用Extension<a name="section107391050141118"></a>

#### 函数声明

- RAW_TO_CHAR(data IN RAW, src_charset IN VARCHAR2 DEFAULT NULL)
  描述：将RAW数据从有效的字符集转换为数据库字符集中的VARCHAR字符串。
  参数详解：data：raw类型数据，src_charset：源字符集。
- GMS_I18N.STRING_TO_RAW(IN strdata varchar2, IN dst_chrset varchar2 DEFAULT NULL)
  描述：将VARCHAR字符串转换为另一个有效的字符集，并将结果作为原始数据返回分。
  参数详解：strdata：需要转换的字符串；dst_chrset：目标字符集。

#### 函数使用

raw_to_char函数

```sql
openGauss=# select gms_i18n.raw_to_char(hextoraw('616263646566C2AA'), 'utf8');
 raw_to_char 
-------------
 abcdefª
(1 row)

```

strin_to_raw函数

```sql
openGauss=# select gms_i18n.string_to_raw('abcdefª', 'utf8');
  string_to_raw   
------------------
 616263646566C2AA
(1 row)

```

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_i18n Extension的方法如下所示：

```
openGauss=# DROP Extension gms_i18n [CASCADE];
```

>[!NOTE]说明
>
>如果Extension被其它对象依赖，需要加入CASCADE（级联）关键字，删除所有依赖对象。
