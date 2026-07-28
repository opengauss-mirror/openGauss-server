# gms_raw

## gms_raw概述

gms_raw是一个基于openGauss的插件，用于对十六进制raw类型数据进行转换和操作。目前支持的接口有：

- gms_raw.bit_and
- gms_raw.bit_or
- gms_raw.bit_complement
- gms_raw.bit_xor
- gms_raw.cast_from_binary_double
- gms_raw.cast_from_binary_float
- gms_raw.cast_from_binary_integer
- gms_raw.cast_from_number
- gms_raw.cast_to_binary_double
- gms_raw.cast_to_binary_float
- gms_raw.cast_to_binary_integer
- gms_raw.cast_to_number
- gms_raw.cast_to_nvarchar2
- gms_raw.cast_to_raw
- gms_raw.cast_to_varchar2
- gms_raw.compare
- gms_raw.concat
- gms_raw.convert
- gms_raw.copies
- gms_raw.reverse
- gms_raw.translate
- gms_raw.transliterate
- gms_raw.xrange

## gms_raw限制

- 仅支持create extension命令方式加载插件。
- gms_raw插件不支持set schema，即执行`alter extension gms_raw set schema new_name;`将提示报错。

## gms_raw安装

openGauss打包编译时默认已经包含了gms_raw插件, 可以在安装完openGauss后，直接通过`create extension gms_raw;`加载插件。

## gms_raw使用

### 创建Extension<a name="section21088306113"></a>

创建gms_raw extension可直接使用create extension命令进行创建：

```
create extension gms_raw;
```

### 使用Extension<a name="section107391050141118"></a>

#### gms_raw.bit_and

- gms_raw.bit_and(r1 in raw, r2 in raw) returns raw

  **描述**：此函数对两个raw类型入参做按位与运算。如果两个raw类型数据长度不一致，按位与运算只执行到较短的数据长度，较长数据的未执行计算部分将直接拼接在结果后面，最终结果的长度与较长的入参长度一致。

  **参数说明**：

  - `r1`: 第一个raw类型入参
  - `r2`: 第二个raw类型入参

  **返回值**：raw数据类型

  **示例**：

  ```
  select gms_raw.bit_and('01','10');
  bit_and 
  ---------
  00
  (1 row)

  select gms_raw.bit_and('01','1010');
  bit_and 
  ---------
  0010
  (1 row)

  select gms_raw.bit_and(null,'01');
  bit_and 
  ---------
  
  (1 row)
  ```

#### gms_raw.bit_or

- gms_raw.bit_or(r1 in raw, r2 in raw) returns raw

  **描述**：此函数对两个raw类型入参做按位或运算。如果两个raw类型数据长度不一致，按位或运算只执行到较短的数据长度，较长数据的未执行计算部分将直接拼接在结果后面，最终结果的长度与较长的入参长度一致。

  **参数说明**：

  - `r1`: 第一个raw类型入参
  - `r2`: 第二个raw类型入参

  **返回值**：raw数据类型

  **示例**：

  ```
  select gms_raw.bit_or('01','10');
  bit_or 
  --------
  11
  (1 row)

  select gms_raw.bit_or('01','1010');
  bit_or 
  --------
  1110
  (1 row)

  select gms_raw.bit_or(null,'01');
  bit_or 
  --------
  
  (1 row)
  ```

#### gms_raw.bit_xor

- gms_raw.bit_xor(r1 in raw, r2 in raw) returns raw

  **描述**：此函数对两个raw类型入参做按位异或运算。如果两个raw类型数据长度不一致，按位异或运算只执行到较短的数据长度，较长数据的未执行计算部分将直接拼接在结果后面，最终结果的长度与较长的入参长度一致。

  **参数说明**：

  - `r1`: 第一个raw类型入参
  - `r2`: 第二个raw类型入参

  **返回值**：raw数据类型

  **示例**：

  ```
  select gms_raw.bit_xor('01','10');
  bit_xor 
  ---------
  11
  (1 row)
  bit_xor 
  ---------
  1110
  (1 row)

  select gms_raw.bit_xor('01','1010');
  bit_xor 
  ---------
  1110
  (1 row)

  select gms_raw.bit_xor(null,'01');
  bit_xor 
  ---------
  
  (1 row)
  ```

#### gms_raw.bit_complement

- gms_raw.bit_complement(r1 in raw) returns raw

  **描述**：此函数对raw类型数据执行按位逻辑补码运算，即取反运算，返回raw结果。

  **参数说明**：

  - `r1`: raw类型入参

  **返回值**：raw数据类型

  **示例**：

  ```
  select gms_raw.bit_complement('1010');
  bit_complement 
  ----------------
  EFEF
  (1 row)

  select gms_raw.bit_complement('0123456789');
  bit_complement 
  ----------------
  FEDCBA9876
  (1 row)

  select gms_raw.bit_complement(null);
  bit_complement 
  ----------------
  
  (1 row)
  ```

#### gms_raw.cast_from_binary_double

- gms_raw.cast_from_binary_double(n in binary_double, endianess in integer default 1) returns raw

  **描述**：此函数将binary_double类型值转换为raw类型。

  **参数说明**：

  - `n`: binary_double类型入参
  - `endianess`: 表示字节顺序的标识符。
  可选参数如下：
  1：大端
  2：小端
  3：机器字节序
  默认值为1，即大端。

  **返回值**：raw数据类型

  **示例**：

  ```
  select * from gms_raw.cast_from_binary_double(3.14);
  cast_from_binary_double 
  -------------------------
  40091EB851EB851F
  (1 row)

  select * from gms_raw.cast_from_binary_double(3.14, 1);
  cast_from_binary_double 
  -------------------------
  40091EB851EB851F
  (1 row)

  select * from gms_raw.cast_from_binary_double(3.14, 2);
  cast_from_binary_double 
  -------------------------
  1F85EB51B81E0940
  (1 row)

  select * from gms_raw.cast_from_binary_double(3.14, 3);
  cast_from_binary_double 
  -------------------------
  1F85EB51B81E0940
  (1 row)
  ```

#### gms_raw.cast_from_binary_float

- gms_raw.cast_from_binary_float(n in float, endianess in integer default 1)

  **描述**：此函数将float类型值转换为raw类型，注意入参1为float类型。

  **参数说明**：

  - `n`: float类型入参
  - `endianess`: 表示字节顺序的标识符。
  可选参数如下：
  1：大端
  2：小端
  3：机器字节序
  默认值为1，即大端。

  **返回值**：raw数据类型

  **示例**：

  ```
  select * from gms_raw.cast_from_binary_float(3.14);
  cast_from_binary_float 
  ------------------------
  4048F5C3
  (1 row)

  select * from gms_raw.cast_from_binary_float(3.14, 1);
  cast_from_binary_float 
  ------------------------
  4048F5C3
  (1 row)

  select * from gms_raw.cast_from_binary_float(3.14, 2);
  cast_from_binary_float 
  ------------------------
  C3F54840
  (1 row)

  select * from gms_raw.cast_from_binary_float(3.14, 3);
  cast_from_binary_float 
  ------------------------
  C3F54840
  (1 row)
  ```

#### gms_raw.cast_from_binary_integer

- gms_raw.cast_from_binary_integer(n in bigint, endianess in integer default 1)

  **描述**：此函数将bigint类型值转换为raw类型，注意入参1为bigint类型。

  **参数说明**：

  - `n`: bigint类型入参
  - `endianess`: 表示字节顺序的标识符。
  可选参数如下：
  1：大端
  2：小端
  3：机器字节序
  默认值为1，即大端。

  **返回值**：raw数据类型

  **说明**：

  - 针对两个入参，当为小数时，会按照四舍五入转化为整形
  - 当入参1的值超过bigint的范围时，将报错bigint out of range

  **示例**：

  ```
  select * from gms_raw.cast_from_binary_integer(3);
  cast_from_binary_integer 
  --------------------------
  00000003
  (1 row)

  select * from gms_raw.cast_from_binary_integer(3, 1);
  cast_from_binary_integer 
  --------------------------
  00000003
  (1 row)

  select * from gms_raw.cast_from_binary_integer(3, 2);
  cast_from_binary_integer 
  --------------------------
  03000000
  (1 row)

  select * from gms_raw.cast_from_binary_integer(3, 3);
  cast_from_binary_integer 
  --------------------------
  03000000
  (1 row)

  select * from gms_raw.cast_from_binary_integer(3.6, 3);
  cast_from_binary_integer 
  --------------------------
  04000000
  (1 row)

  select * from gms_raw.cast_from_binary_integer(3.3, 3.3);
  cast_from_binary_integer 
  --------------------------
  03000000
  ```

#### gms_raw.cast_from_number

- gms_raw.cast_from_number(n in number) returns raw

  **描述**：此函数将number类型值转换为raw类型。

  **参数说明**：

  - `n`: number类型入参

  **返回值**：raw数据类型

  **说明**：

  - 转换结果取决于openGauss自身对number类型数据的存储方式

  **示例**：

  ```
  select * from gms_raw.cast_from_number(3.14);
  cast_from_number 
  ------------------
  008103007805
  (1 row)

  select * from gms_raw.cast_from_number(3.1415926);
  cast_from_number 
  ------------------
  8083030087052C24
  (1 row)

  select * from gms_raw.cast_from_number(100);
  cast_from_number 
  ------------------
  00806400
  (1 row)

  select * from gms_raw.cast_from_number(3e100);
  cast_from_number 
  ------------------
  19800300
  (1 row)
  ```

#### gms_raw.cast_to_binary_double

- gms_raw.cast_to_binary_double(r in raw, endianess in integer default 1) returns binary_double

  **描述**：此函数将raw类型值转化为binary_double类型。

  **参数说明**：

  - `r`: raw类型入参
  - `endianess`: 表示字节顺序的标识符。
  可选参数如下：
  1：大端
  2：小端
  3：机器字节序
  默认值为1，即大端。

  **返回值**：binary_double数据类型

  **示例**：

  ```
  select * from gms_raw.cast_from_binary_double(3.14, 1);
  cast_from_binary_double 
  -------------------------
  40091EB851EB851F
  (1 row)

  select gms_raw.cast_to_binary_double('40091EB851EB851F');
  cast_to_binary_double 
  -----------------------
                    3.14
  (1 row)

  select gms_raw.cast_to_binary_double('40091EB851EB851F', 1);
  cast_to_binary_double 
  -----------------------
                    3.14
  (1 row)

  select gms_raw.cast_to_binary_double('1F85EB51B81E0940', 2);
  cast_to_binary_double 
  -----------------------
                    3.14
  (1 row)

  select gms_raw.cast_to_binary_double('1F85EB51B81E0940', 3);
  cast_to_binary_double 
  -----------------------
                    3.14
  (1 row)
  ```

#### gms_raw.cast_to_binary_float

- gms_raw.cast_to_binary_float(r in raw, endianess in integer default 1) returns float4

  **描述**：此函数将raw类型值转化为float4类型，注意返回值为float4类型。

  **参数说明**：

  - `r`: raw类型入参
  - `endianess`: 表示字节顺序的标识符。
  可选参数如下：
  1：大端
  2：小端
  3：机器字节序
  默认值为1，即大端。

  **返回值**：float4数据类型

  **示例**：

  ```
  select * from gms_raw.cast_from_binary_float(3.14, 1);
  cast_from_binary_float 
  ------------------------
  4048F5C3
  (1 row)

  select gms_raw.cast_to_binary_float('4048F5C3');
  cast_to_binary_float 
  ----------------------
                  3.14
  (1 row)

  select gms_raw.cast_to_binary_float('4048F5C3', 1);
  cast_to_binary_float 
  ----------------------
                  3.14
  (1 row)

  select gms_raw.cast_to_binary_float('C3F54840', 2);
  cast_to_binary_float 
  ----------------------
                  3.14
  (1 row)

  select gms_raw.cast_to_binary_float('C3F54840', 3);
  cast_to_binary_float 
  ----------------------
                  3.14
  (1 row)
  ```

#### gms_raw.cast_to_binary_integer

- gms_raw.cast_to_binary_integer(r in raw, endianess in integer default 1) returns binary_integer

  **描述**：此函数将raw类型值转化为binary_integer类型。

  **参数说明**：

  - `r`: raw类型入参
  - `endianess`: 表示字节顺序的标识符。
  可选参数如下：
  1：大端
  2：小端
  3：机器字节序
  默认值为1，即大端。

  **返回值**：binary_integer数据类型

  **示例**：

  ```
  select gms_raw.cast_from_binary_integer(3);
  cast_from_binary_integer 
  --------------------------
  00000003
  (1 row)

  select gms_raw.cast_to_binary_integer('00000003');
  cast_to_binary_integer 
  ------------------------
                        3
  (1 row)

  select gms_raw.cast_to_binary_integer('00000003', 1);
  cast_to_binary_integer 
  ------------------------
                        3
  (1 row)

  select gms_raw.cast_to_binary_integer('03000000', 2);
  cast_to_binary_integer 
  ------------------------
                        3
  (1 row)

  select gms_raw.cast_to_binary_integer('03000000', 3);
  cast_to_binary_integer 
  ------------------------
                        3
  (1 row)
  ```

#### gms_raw.cast_to_number

- gms_raw.cast_to_number(r in raw) returns number

  **描述**：此函数将raw类型值转化为number类型。

  **参数说明**：

  - `r`: raw类型入参

  **返回值**：number数据类型

  **示例**：

  ```
  select * from gms_raw.cast_from_number(3.14);
  cast_from_number 
  ------------------
  008103007805
  (1 row)

  select * from gms_raw.cast_to_number('008103007805');
  cast_to_number 
  ----------------
            3.14
  (1 row)
  ```

#### gms_raw.cast_to_nvarchar2

- gms_raw.cast_to_nvarchar2(r in raw) returns nvarchar2

  **描述**：此函数将raw类型值转化为nvarchar2类型。

  **参数说明**：

  - `r`: raw类型入参

  **返回值**：nvarchar2数据类型

  **示例**：

  ```
  select * from gms_raw.cast_to_nvarchar2('12345678');
  cast_to_nvarchar2 
  -------------------
  \x124Vx
  (1 row)
  ```

#### gms_raw.cast_to_raw

- gms_raw.cast_to_raw(c in varchar2) returns raw

  **描述**：此函数将varchar2类型值转化为raw类型。

  **参数说明**：

  - `c`: varchar2类型入参

  **返回值**：raw数据类型

  **示例**：

  ```
  select * from gms_raw.cast_to_raw('12345');
  cast_to_raw 
  -------------
  3132333435
  (1 row)

  select * from gms_raw.cast_to_raw('abcdefghijklmn');
          cast_to_raw          
  ------------------------------
  6162636465666768696A6B6C6D6E
  (1 row)

  select * from gms_raw.cast_to_raw('你好明天');
        cast_to_raw        
  --------------------------
  E4BDA0E5A5BDE6988EE5A4A9
  (1 row)

  select * from gms_raw.cast_to_raw('你好openGauss');
            cast_to_raw           
  --------------------------------
  E4BDA0E5A5BD6F70656E4761757373
  (1 row)
  ```

#### gms_raw.cast_to_varchar2

- gms_raw.cast_to_varchar2(r in raw) returns varchar2

  **描述**：此函数将raw类型值转化为varchar2类型。

  **参数说明**：

  - `r`: raw类型入参

  **返回值**：varchar2数据类型

  **示例**：  

  ```
  select * from gms_raw.cast_to_raw('12345');
  cast_to_raw 
  -------------
  3132333435
  (1 row)

  select * from gms_raw.cast_to_varchar2('3132333435');
  cast_to_varchar2 
  ------------------
  12345
  (1 row)

  select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('12345'));
  cast_to_varchar2 
  ------------------
  12345
  (1 row)

  select * from gms_raw.cast_to_raw('abcdefghijklmn');
          cast_to_raw          
  ------------------------------
  6162636465666768696A6B6C6D6E
  (1 row)

  select * from gms_raw.cast_to_varchar2('6162636465666768696A6B6C6D6E');
  cast_to_varchar2 
  ------------------
  abcdefghijklmn
  (1 row)

  select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('abcdefghijklmn'));
  cast_to_varchar2 
  ------------------
  abcdefghijklmn
  (1 row)

  select * from gms_raw.cast_to_raw('你好明天');
        cast_to_raw        
  --------------------------
  E4BDA0E5A5BDE6988EE5A4A9
  (1 row)

  select * from gms_raw.cast_to_varchar2('E4BDA0E5A5BDE6988EE5A4A9');
  cast_to_varchar2 
  ------------------
  你好明天
  (1 row)

  select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('你好明天'));
  cast_to_varchar2 
  ------------------
  你好明天
  (1 row)

  select * from gms_raw.cast_to_raw('你好openGauss');
            cast_to_raw           
  --------------------------------
  E4BDA0E5A5BD6F70656E4761757373
  (1 row)

  select * from gms_raw.cast_to_varchar2('E4BDA0E5A5BD6F70656E4761757373');
  cast_to_varchar2 
  ------------------
  你好openGauss
  (1 row)

  select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('你好openGauss'));
  cast_to_varchar2 
  ------------------
  你好openGauss
  (1 row)
  ```

#### gms_raw.compare

- gms_raw.compare(r1 in raw, r2 in raw, pad in raw default null) returns number

  **描述**：此函数对两个raw值进行比较，返回第一个不相等的字节的位置（从1开始），如果两个raw值相等，则返回0。当两个raw值长度不同时，会根据可选参数pad的首字节填充较短的参数，直至长度一致。可选参数的默认值为null，即`x'00'`。

  **参数说明**：

  - `r1`: 第一个raw类型入参
  - `r2`: 第二个raw类型入参
  - `pad`: 可选raw参数，当两个raw值长度不同时，会根据可选参数pad的首字节填充较短的参数，直至长度一致。默认值为null，即`x'00'`。

  **返回值**：number数据类型

  **示例**：

  ```
  select gms_raw.compare('', '01');
  compare 
  ---------
        1
  (1 row)

  select gms_raw.compare(NULL, '01');
  compare 
  ---------
        1
  (1 row)

    select gms_raw.compare('01', '', '0123');
  compare 
  ---------
        0
  (1 row)

  select gms_raw.compare('01', '0123', '0');
  compare 
  ---------
        2
  (1 row)
  ```

#### gms_raw.concat

- gms_raw.concat(r1 in raw default null, r2 in raw default null, r3 in raw default null, r4 in raw default null,
                 r5 in raw default null, r6 in raw default null, r7 in raw default null, r8 in raw default null,
                 r9 in raw default null, r10 in raw default null, r11 in raw default null, r12 in raw default null
                ) returns raw

  **描述**：此函数对最多12个raw类型数据进行拼接，返回拼接后的raw数据。

  **参数说明**：

  - `r1`: 第一个raw类型入参，默认值为null
  - `r2`: 第二个raw类型入参，默认值为null
  - `r3`: 第三个raw类型入参，默认值为null
  - `r4`: 第四个raw类型入参，默认值为null
  - `r5`: 第五个raw类型入参，默认值为null
  - `r6`: 第六个raw类型入参，默认值为null
  - `r7`: 第七个raw类型入参，默认值为null
  - `r8`: 第八个raw类型入参，默认值为null
  - `r9`: 第九个raw类型入参，默认值为null
  - `r10`: 第十个raw类型入参，默认值为null
  - `r11`: 第十一个raw类型入参，默认值为null
  - `r12`: 第十二个raw类型入参，默认值为null

  **返回值**：raw数据类型

  **示例**：

  ```
  select gms_raw.concat();
  concat 
  --------
  
  (1 row)

  select gms_raw.concat('11'); 
  concat 
  --------
  11
  (1 row)

  select gms_raw.concat('00', '11');
  concat 
  --------
  0011
  (1 row)

  select gms_raw.concat('11', '22', '33');
  concat 
  --------
  112233
  (1 row)

  select gms_raw.concat('11', '22', '33', '44');
    concat  
  ----------
  11223344
  (1 row)

  select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', '99', '00', 'aa', 'bb');
            concat          
  --------------------------
  11223344556677889900AABB
  (1 row)

  select gms_raw.concat(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  concat 
  --------
  
  (1 row)
  ```

#### gms_raw.convert

- gms_raw.convert(r in raw, to_charset in varchar2, from_charset in varchar2) returns raw

  **描述**：此函数将一个raw值从from_charset字符集转换到to_charset字符集。

  **参数说明**：

  - `r`: raw类型入参，要转换的raw数据
  - `to_charset`: varchar2类型入参，目标字符集
  - `from_charset`: varchar2类型入参，源字符集

  **返回值**：raw数据类型，转换后的raw值

  **说明**：

  - from_charset和to_charset必须是openGauss支持的字符集

  **示例**：

  ```
  select gms_raw.convert('31', 'utf8', 'utf8');
  convert 
  ---------
  31
  (1 row)

  select gms_raw.convert('31', 'gbk', 'gbk');
  convert 
  ---------
  31
  (1 row)

  select gms_raw.convert('31', 'utf8', 'gbk');
  convert 
  ---------
  31
  (1 row)

  select * from gms_raw.cast_to_raw('你好明天');
        cast_to_raw        
  --------------------------
  E4BDA0E5A5BDE6988EE5A4A9
  (1 row)

  select * from gms_raw.convert('E4BDA0E5A5BDE6988EE5A4A9', 'gbk', 'gbk');
          convert          
  --------------------------
  E4BDA0E5A5BDE6988EE5A4A9
  (1 row)

  select * from gms_raw.convert('E4BDA0E5A5BDE6988EE5A4A9', 'utf8', 'utf8');
          convert          
  --------------------------
  E4BDA0E5A5BDE6988EE5A4A9
  (1 row)

  select * from gms_raw.convert('E4BDA0E5A5BDE6988EE5A4A9', 'utf8', 'gbk');
                convert                
  --------------------------------------
  E6B5A3E78AB2E382BDE98F84E5BAA1E38189
  (1 row)

  select * from gms_raw.convert('E6B5A3E78AB2E382BDE98F84E5BAA1E38189', 'gbk', 'utf8');
          convert          
  --------------------------
  E4BDA0E5A5BDE6988EE5A4A9
  (1 row)

  select * from gms_raw.cast_to_varchar2('E4BDA0E5A5BDE6988EE5A4A9');
  cast_to_varchar2 
  ------------------
  你好明天
  (1 row)
  ```

#### gms_raw.copies

- gms_raw.copies(r in raw, n in number) returns raw

  **描述**：此函数将一个raw值复制n份，拼接在一起返回。

  **参数说明**：

  - `r`: raw类型入参，要复制的raw数据
  - `n`: number类型入参，复制的次数

  **返回值**：raw数据类型，复制后的raw值

  **说明**：
  
  - 当第一个参数r为null或空字符串''，将报错提示
  - 当第二个参数n为null或空字符串''，将报错提示
  - 当第二个参数n为小数时，将进行四舍五入转化为整数
  - 当第二个参数n转化为整数后不在[1, 1073733617]范围内，将报错提示
  - 当复制后的长度大于raw类型的最大长度1073733617，将报错提示

  **示例**：

  ```
  select gms_raw.copies('001122', 1);
  copies 
  --------
  001122
  (1 row)

  select gms_raw.copies('001122', 3);
        copies       
  --------------------
  001122001122001122
  (1 row)

  select gms_raw.copies('00112233', 3);
            copies          
  --------------------------
  001122330011223300112233
  (1 row)

  select gms_raw.copies('00112233', 3.2);
            copies          
  --------------------------
  001122330011223300112233
  (1 row)

  select gms_raw.copies('00112233', 3.6);
                copies              
  ----------------------------------
  00112233001122330011223300112233
  (1 row)
  ```

#### gms_raw.reverse

- gms_raw.reverse(r in raw) returns raw

  **描述**：此函数将一个raw值按字节进行反转。

  **参数说明**：

  - `r`: raw类型入参，要反转的raw数据

  **返回值**：raw数据类型，反转后的raw值

  **说明**：
  
  - 当第一个参数r为null或空字符串''，将报错提示

  **示例**：

  ```
  select gms_raw.reverse('1');
  reverse 
  ---------
  01
  (1 row)

  select gms_raw.reverse('01');
  reverse 
  ---------
  01
  (1 row)

  select gms_raw.reverse('1122');
  reverse 
  ---------
  2211
  (1 row)

  select gms_raw.reverse('11223344');
  reverse  
  ----------
  44332211
  (1 row)

  select gms_raw.reverse('12345678');
  reverse  
  ----------
  78563412
  (1 row)
  ```

#### gms_raw.translate

- gms_raw.translate(r in raw, from_set in raw, to_set in raw) returns raw

  **描述**：此函数将raw值逐字节按照from_set到to_set进行替换，返回新的raw值。比对from_set时以从左到右出现的第一个相同字节为准，后续重复出现的将被忽略。如果raw值的数据在from_set存在，但在to_set中对应位置无数据，则删除该字节。
如果raw值的数据在from_set不存在，则直接复制到返回结果中。

  **参数说明**：

  - `r`: raw类型入参，替换前的raw数据
  - `from_set`: raw类型入参，要替换的字节数据
  - `to_set`: raw类型入参，替换后的字节数据

  **返回值**：raw数据类型，按照from_set到to_set替换后的raw数据

  **说明**：
  
  - 当第一个参数r为null或空字符串''，将报错提示
  - 当第二个参数from_set为null或空字符串''，将报错提示
  - 当第三个参数to_set为null或空字符串''，将报错提示

  **示例**：

  ```
  select gms_raw.translate('1100110011', '11', '12');
  translate  
  ------------
  1200120012
  (1 row)

  select gms_raw.translate('1100110011', '1100', '12');
  translate 
  -----------
  121212
  (1 row)

  select gms_raw.translate('01110011100111','011111','123456');
    translate    
  ----------------
  12340034101234
  (1 row)

  select gms_raw.translate('aabbccdd001122','aabbccdd','eeff');
  translate  
  ------------
  EEFF001122
  (1 row)

  select gms_raw.translate('aabbccdd001122','aabbccdd','eeff001133');
    translate    
  ----------------
  EEFF0011001122
  (1 row)

  select gms_raw.translate('aabbccdd001122','aaaabbcc','eeff0011');
    translate    
  ----------------
  EE0011DD001122
  (1 row)
  ```

#### gms_raw.transliterate

- gms_raw.transliterate(r in raw, to_set in raw default null, from_set in raw default null, pad in raw default null) returns raw

  **描述**：此函数将raw值逐字节按照from_set到to_set进行替换，返回新的raw值。比对from_set时以从左到右出现的第一个相同字节为准，后续重复出现的将被忽略。如果raw值的数据在from_set存在，但在to_set中对应位置无数据，
则使用pad参数的首字节替换。如果raw值的数据在from_set不存在，则直接复制到返回结果中。

  **参数说明**：

  - `r`: raw类型入参，替换前的raw数据
  - `to_set`: raw类型入参，替换后的字节数据，默认值为null，即`x'00'`
  - `from_set`: raw类型入参，要替换的字节数据，默认值为null，即`x'00'`
  - `pad`: raw类型入参，默认替换值，只会用到首字节替换，默认值为null，即`x'00'`

  **返回值**：raw数据类型，结合pad，按照from_set到to_set替换后的raw数据

  **说明**：

  - 当第一个参数r为null或空字符串''，将报错提示
  - 当第二个参数from_set为null或空字符串''，参数r的每个字节将被pad参数的首字节替换并返回
  - gms_raw.transliterate函数两个入参from_set和to_set的位置和gms_raw.translate接口相反

  **示例**：

  ```
  select gms_raw.transliterate('aabbccddeeffaabbccddeeff');
        transliterate       
  --------------------------
  000000000000000000000000
  (1 row)

  select gms_raw.transliterate('aabbccddeeffaabbccddeeff', NULL);
        transliterate       
  --------------------------
  000000000000000000000000
  (1 row)

  select gms_raw.transliterate('aabbccddeeffaabbccddeeff', '');
        transliterate       
  --------------------------
  000000000000000000000000
  (1 row)

  select gms_raw.transliterate('aabbccddee','bb', 'cc', 'aa');
  transliterate 
  ---------------
  AABBBBDDEE
  (1 row)

  select gms_raw.transliterate('aabbccddee','bb', 'ccee', 'aa');
  transliterate 
  ---------------
  AABBBBDDAA
  (1 row)

  select gms_raw.transliterate('aabbccddee','bb', 'ccee', 'aabb');
  transliterate 
  ---------------
  AABBBBDDAA
  (1 row)

  select gms_raw.transliterate('aabbccddee','bbddff', 'ccee', 'aa');
  transliterate 
  ---------------
  AABBBBDDDD
  (1 row)

  select gms_raw.transliterate('aabbccddee','bbddff', 'ccee', 'aabb');
  transliterate 
  ---------------
  AABBBBDDDD
  (1 row)

  select gms_raw.transliterate('aabbccddeeff','bbddff', 'ccee', 'aabb');
  transliterate 
  ---------------
  AABBBBDDDDFF
  (1 row)

  select gms_raw.transliterate('aabbccddeeff','bbddff11', 'cceeccee', 'aabb');
  transliterate 
  ---------------
  AABBBBDDDDFF
  (1 row)
  ```

#### gms_raw.xrange

- gms_raw.xrange(start_byte in raw default null, end_byte in raw default null) returns raw

  **描述**：此函数从00到ff的收尾循环raw数据中返回[start_byte, end_byte]范围的raw数据。

  **参数说明**：

  - `start_byte`: raw类型入参，起始raw数据，只用首字节，默认值为null，即`x'00'`
  - `end_byte`: raw类型入参，结尾raw数据，只取首字节，默认值为null，即`x'ff'`

  **返回值**：raw数据类型，返回[start_byte, end_byte]范围的raw数据

  **说明**：

  - 当第一个参数r为null或空字符串''，将报错提示
  - 当第二个参数from_set为null或空字符串''，参数r的每个字节将被pad参数的首字节替换并返回
  - gms_raw.transliterate函数两个入参from_set和to_set的位置和gms_raw.translate接口相反

  **示例**：

```
select gms_raw.xrange(NULL, '08');
       xrange       
--------------------
 000102030405060708
(1 row)

select gms_raw.xrange('', '08');
       xrange       
--------------------
 000102030405060708
(1 row)

select gms_raw.xrange('33', '33');
 xrange 
--------
 33
(1 row)

select gms_raw.xrange('33', '44');
                xrange                
--------------------------------------
 333435363738393A3B3C3D3E3F4041424344
(1 row)

select gms_raw.xrange('33', '33');
 xrange 
--------
 33
(1 row)

select gms_raw.xrange('3311', '4422');
                xrange                
--------------------------------------
 333435363738393A3B3C3D3E3F4041424344
(1 row)
```

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_raw extension的方法如下所示：

```
drop extension gms_raw [cascade];
```

>[!NOTE]说明
>
>如果extension被其它对象依赖，需要加入cascade（级联）关键字，删除所有依赖对象。
