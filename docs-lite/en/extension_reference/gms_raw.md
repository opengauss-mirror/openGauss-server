# gms_raw

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:27:01.611Z pushedAt=2026-09-20T08:43:32.738Z -->

## gms_raw Overview

gms_raw is a plugin based on openGauss, used for converting and manipulating hexadecimal raw type data. The currently supported interfaces are:

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

## gms_raw Limitations

- The plugin can only be loaded using the CREATE EXTENSION command.
- The gms_raw plugin does not support SET SCHEMA, meaning that executing `alter extension gms_raw set schema new_name;` will report an error.

## gms_raw Installation

The gms_raw extension is included by default during openGauss packaging and compilation. After openGauss is installed, the extension can be loaded directly by executing <code>create extension gms_raw;</code>.

## gms_raw Usage

### Creating an Extension<a name="section21088306113"></a>

To create the gms_raw extension, you can directly use the create extension command:

```
create extension gms_raw;
```

### Using Extension<a name="section107391050141118"></a>

#### gms_raw.bit_and

- gms_raw.bit_and(r1 in raw, r2 in raw) returns raw

  **Description**: This function performs a bitwise AND operation on two raw type input parameters. If the lengths of the two raw type data values differ, the bitwise AND operation is performed only up to the length of the shorter data, and the unprocessed portion of the longer data is directly appended to the result. The length of the final result is consistent with that of the longer input parameter.

  **Parameter description**:

  - `r1`: the first raw type input parameter
  - `r2`: second raw type input parameter

  **Return value**: raw data type

  **Example**:

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

  **Description**: This function performs a bitwise OR operation on two raw type input parameters. If the lengths of the two raw type data values differ, the bitwise OR operation is performed only up to the length of the shorter data, and the unprocessed portion of the longer data is directly concatenated to the end of the result. The length of the final result is consistent with that of the longer input parameter.

  **Parameter Description**:

  - `r1`: The first raw type input parameter
  - `r2`: second raw type input parameter

  **Return value**: raw data type

  **Example**:

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

  **Description**: This function performs a bitwise XOR operation on two raw type input parameters. If the lengths of the two raw type data values differ, the bitwise XOR operation is performed only up to the length of the shorter data, and the unprocessed portion of the longer data is directly appended to the result. The length of the final result is consistent with that of the longer input parameter.

  **Parameter Description**:

  - `r1`: the first raw type input parameter
  - `r2`: second raw type input parameter

  **Return value**: raw data type

  **Example**:

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

  **Description**: This function performs a bitwise logical complement operation (i.e., negation) on raw type data and returns the raw result.

  **Parameter Description**:

  - `r1`: raw type input parameter

  **Return value**: raw data type

  **Example**:

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

  **Description**: This function converts a binary_double type value to the raw type.

  **Parameter description**:

  - `n`: binary_double type input parameter
  - `endianess`: Identifier that represents the byte order.
  Optional parameters are as follows: 1: big-endian 2: little-endian 3: machine byte order The default value is 1, i.e., big-endian.

  **Return value**: raw data type

  **Example**:

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

  **Description**: This function converts a float type value to raw type. Note that the first input parameter is of float type.

  **Parameter description**:

  - `n`: float type input parameter
  - `endianess`: An identifier that represents the byte order.
  Optional parameters are as follows: 
1: big-endian 
2: little-endian 
3: machine byte order 
The default value is 1, i.e., big-endian.

  **Return value**: raw data type

  **Example**:

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

  **Description**: This function converts a bigint type value to raw type. Note that the first input parameter is of bigint type.

  **Parameter description**:

  - `n`: bigint type input parameter
  - `endianess`: identifier that represents the byte order.
  Optional parameters are as follows: 1: big-endian 2: little-endian 3: machine byte order. The default value is 1, i.e., big-endian.

  **Return value**: raw data type

  **NOTE**

  - For the two input parameters, if they are decimal values, they will be rounded to integers.
  - When the value of input parameter 1 exceeds the range of bigint, an error "bigint out of range" will be reported.

  **Example**:

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

  **Description**: This function converts a number type value to the raw type.

  **Parameter Description**:

  - `n`: number type input parameter

  **Return value**: raw data type

  **NOTE**

  - The conversion result depends on how openGauss stores number type data internally.

  **Example**:

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

  **Description**: This function converts a raw type value to the binary_double type.

  **Parameter description**:

  - `r`: raw type input parameter
  - `endianess`: Identifier that represents the byte order.
  Optional parameters are as follows: 1: big-endian 2: little-endian 3: machine byte order The default value is 1, i.e., big-endian.

  **Return value**: binary_double data type

  **Example**:

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

  **Description**: This function converts a raw type value to the float4 type. Note that the return value is of the float4 type.

  **Parameter description**:

  - `r`: raw type input parameter
  - `endianess`: A flag that indicates the byte order.
  Optional parameters are as follows:
  1: big-endian
  2: little-endian
  3: machine byte order
  The default value is 1, i.e., big-endian.

  **Return value**: float4 data type

  **Example**:

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

  **Description**: This function converts a raw type value to the binary_integer type.

  **Parameter Description**:

  - `r`: raw type input parameter
  - `endianess`: identifier that represents the byte order.
  Optional parameters are as follows:
  1: big-endian
  2: little-endian
  3: machine byte order
  The default value is 1, i.e., big-endian.

  **Return value**: binary_integer data type

  **Example**:

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

  **Description**: This function converts a raw type value to the number type.

  **Parameter description**:

  - `r`: raw type input parameter

  **Return value**: number data type

  **Example**:

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

  **Description**: This function converts a raw type value to the nvarchar2 type.

  **Parameter Description**:

  - `r`: raw type input parameter

  **Return value**: nvarchar2 data type

  **Example**:

  ```
  select * from gms_raw.cast_to_nvarchar2('12345678');
  cast_to_nvarchar2 
  -------------------
  \x124Vx
  (1 row)
  ```

#### gms_raw.cast_to_raw

- gms_raw.cast_to_raw(c in varchar2) returns raw

  **Description**: This function converts a varchar2 type value to raw type.

  **Parameter description**:

  - `c`: varchar2 type input parameter

  **Return value**: raw data type

  **Example**:

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

  select * from gms_raw.cast_to_raw('Hello tomorrow');
        cast_to_raw        
  --------------------------
  E4BDA0E5A5BDE6988EE5A4A9
  (1 row)

  select * from gms_raw.cast_to_raw('Hello openGauss');
            cast_to_raw           
  --------------------------------
  E4BDA0E5A5BD6F70656E4761757373
  (1 row)
  ```

#### gms_raw.cast_to_varchar2

- gms_raw.cast_to_varchar2(r in raw) returns varchar2

  **Description**: This function converts a raw type value to varchar2 type.

  **Parameter description**:

  - `r`: raw type input parameter

  **Return value**: varchar2 data type

  **Example**:  

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

  select * from gms_raw.cast_to_raw('Hello tomorrow');
        cast_to_raw        
  --------------------------
  E4BDA0E5A5BDE6988EE5A4A9
  (1 row)

  select * from gms_raw.cast_to_varchar2('E4BDA0E5A5BDE6988EE5A4A9');
  cast_to_varchar2 
  ------------------
  你好明天
  (1 row)

  select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('Hello tomorrow'));
  cast_to_varchar2 
  ------------------
  你好明天
  (1 row)

  select * from gms_raw.cast_to_raw('Hello openGauss');
            cast_to_raw           
  --------------------------------
  E4BDA0E5A5BD6F70656E4761757373
  (1 row)

  select * from gms_raw.cast_to_varchar2('E4BDA0E5A5BD6F70656E4761757373');
  cast_to_varchar2 
  ------------------
  你好openGauss
  (1 row)

  select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('Hello openGauss'));
  cast_to_varchar2 
  ------------------
  你好openGauss
  (1 row)
  ```

#### gms_raw.compare

- gms_raw.compare(r1 in raw, r2 in raw, pad in raw default null) returns number

  **Description**: This function compares two raw values and returns the position (starting from 1) of the first unequal byte. If the two raw values are equal, 0 is returned. When the two raw values have different lengths, the shorter parameter is padded with the first byte of the optional parameter `pad` until the lengths are equal. The default value of the optional parameter is null, i.e., `x'00'`.

  **Parameter Description**:

  - `r1`: the first raw type input parameter
  - `r2`: second raw type input parameter
  - `pad`: optional raw parameter. When the two raw values have different lengths, the shorter parameter is padded with the first byte of the optional parameter `pad` until the lengths are equal. The default value is null, i.e., `x'00'`.

  **Return value**: number data type

  **Example**:

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

  **Description**: This function concatenates up to 12 raw type data and returns the concatenated raw data.

  **Parameter description**:

  - `r1`: the first raw type input parameter, default value is null
  - `r2`: the second raw type input parameter, default value is null
  - `r3`: the third raw type input parameter, default value is null
  - `r4`: the fourth raw type input parameter, default value is null
  - `r5`: the fifth raw type input parameter, default value is null
  - `r6`: the sixth raw type input parameter, default value is null
  - `r7`: the seventh raw type input parameter, default value is null
  - `r8`: the eighth raw type input parameter, default value is null
  - `r9`: the ninth raw type input parameter, default value is null
  - `r10`: the tenth raw type input parameter, default value is null
  - `r11`: the eleventh raw type input parameter, default value is null
  - `r12`: the twelfth raw type input parameter, default value is null

  **Return value**: raw data type

  **Example**:

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

  **Description**: This function converts a raw value from the from_charset character set to the to_charset character set.

  **Parameter Description**:

  - `r`: raw type input parameter, the raw data to be converted
  - `to_charset`: varchar2 type input parameter, the target character set
  - `from_charset`: varchar2 type input parameter, the source character set

  **Return value**: raw data type, the converted raw value

  **NOTE**

  - from_charset and to_charset must be character sets supported by openGauss

  **Example**:

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

  select * from gms_raw.cast_to_raw('Hello tomorrow');
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

  **Description**: This function copies a raw value n times and concatenates them together for return.

  **Parameter Description**:

  - `r`: raw type input parameter, the raw data to be copied
  - `n`: number type input parameter, the number of times to copy

  **Return value**: raw data type, the raw value after copying

  **NOTE**

  - When the first parameter r is null or an empty string '', an error will be reported
  - When the second parameter n is null or an empty string '', an error will be reported
  - When the second parameter n is a decimal, it will be rounded and converted to an integer.
  - When the second parameter n, after being converted to an integer, is not within the range [1, 1073733617], an error will be reported.
  - When the length after copying exceeds the maximum length of the raw type, 1073733617, an error will be reported.

  **Example**:

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

  **Description**: This function reverses a raw value byte by byte.

  **Parameter Description**:

  - `r`: raw type input parameter, the raw data to be reversed

  **Return value**: raw data type, the reversed raw value

  **NOTE**

  - When the first parameter r is null or an empty string '', an error will be reported

  **Example**:

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

  **Description**: This function replaces each byte of the raw value from from_set to to_set and returns a new raw value. When matching against from_set, the first identical byte encountered from left to right takes precedence, and subsequent duplicates are ignored. If the data of the raw value exists in from_set but there is no data at the corresponding position in to_set, that byte is deleted. If the data of the raw value does not exist in from_set, it is directly copied to the returned result.

  **Parameter description**:

  - `r`: raw type input parameter, the raw data before replacement
  - `from_set`: raw type input parameter, the byte data to be replaced
  - `to_set`: raw type input parameter, the replaced byte data

  **Return value**: raw data type, the raw data after replacement from from_set to to_set

  **NOTE**

  - When the first parameter r is null or an empty string '', an error will be reported
  - When the second parameter from_set is null or an empty string '', an error will be reported.
  - When the third parameter to_set is null or an empty string '', an error will be reported.

  **Example**:

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

  **Description**: This function replaces each byte of the raw value according to the mapping from from_set to to_set, and returns a new raw value. When matching against from_set, the first identical byte encountered from left to right is used, and subsequent duplicates are ignored. If the data of the raw value exists in from_set but there is no data at the corresponding position in to_set, the first byte of the pad parameter is used for replacement. If the data of the raw value does not exist in from_set, it is directly copied to the returned result.

  **Parameter description**:

  - `r`: raw type input parameter, the raw data before replacement
  - `to_set`: raw type input parameter, the replaced byte data. The default value is null, i.e., `x'00'`
  - `from_set`: raw type input parameter, the byte data to be replaced. The default value is null, i.e., `x'00'`
  - `pad`: raw type input parameter, the default replacement value. Only the first byte is used for replacement. The default value is null, i.e., `x'00'`

  **Return value**: raw data type, the raw data after replacement from `from_set` to `to_set` combined with `pad`

  **NOTE**

  - When the first parameter r is null or an empty string '', an error will be reported.
  - When the second parameter from_set is null or an empty string '', each byte of parameter r will be replaced by the first byte of the pad parameter and returned.
  - The positions of the two input parameters from_set and to_set in the gms_raw.transliterate function are opposite to those in the gms_raw.translate interface.

  **Example**:

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

  **Description**: This function returns the raw data within the [start_byte, end_byte] range from the cyclic raw data spanning from 00 to ff.

  **Parameter description**:

  - `start_byte`: raw type input parameter, the starting raw data. Only the first byte is used. The default value is null, i.e., `x'00'`.
  - `end_byte`: raw type input parameter, the ending raw data. Only the first byte is used. The default value is null, i.e., `x'ff'`

  **Return value**: raw data type. Returns the raw data within the range [start_byte, end_byte]

  **NOTE**

  - When the first parameter r is null or an empty string '', an error will be reported
  - When the second parameter from_set is null or an empty string '', each byte of parameter r will be replaced by the first byte of the pad parameter and returned
  - The positions of the two input parameters from_set and to_set in the gms_raw.transliterate function are opposite to those in the gms_raw.translate interface.

  **Example**:

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

### Deleting an Extension<a name="section1587441381220"></a>

The method for deleting the gms_raw extension in openGauss is as follows:

```
drop extension gms_raw [cascade];
```

>[!NOTE] Note
>
>If the extension is depended on by other objects, the cascade keyword must be added to delete all dependent objects.