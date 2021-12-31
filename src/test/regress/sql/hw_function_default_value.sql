create database pl_test_func_default DBCOMPATIBILITY 'pg';
\c pl_test_func_default;
DROP FUNCTION IF EXISTS test_add1(INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_select(INTEGER, INTEGER[]);
CREATE OR REPLACE FUNCTION test_add1(i IN INTEGER, j IN INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= j + i;
	RETURN temp;
END;
/
CREATE OR REPLACE FUNCTION test_select(i IN INTEGER, VARIADIC arr INTEGER[]) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= arr[i];
	RETURN temp;
END;
/
--/* test for "select" */
SELECT test_add1(1,2);
SELECT test_add1(1,j:=2);
SELECT test_add1(i:=1,j:=2);
SELECT test_add1(i=>1,j:=2);
SELECT test_add1(i:=1,j=>2);
SELECT test_add1(i=>1,j=>2);
SELECT test_select(1,2);
SELECT test_select(2,VARIADIC ARRAY[1,2,3,4]);
SELECT test_select(i:=1,VARIADIC arr=>ARRAY[1,2,3,4]);
SELECT test_select(i=>2,VARIADIC arr:=ARRAY[1,2,3,4]);
SELECT test_select(i:=3,VARIADIC arr:=ARRAY[1,2,3,4]);
SELECT test_select(i=>4,VARIADIC arr=>ARRAY[1,2,3,4]);
SELECT test_select(i:=2,VARIADIC ARRAY[1,2,3,4]);
--/* test for "call" */
CALL test_add1(1,2);
CALL test_add1(1,j:=2);
CALL test_add1(i:=1,j:=2);
CALL test_add1(i=>1,j:=2);
CALL test_add1(i:=1,j=>2);
CALL test_add1(i=>1,j=>2);
--/* clean up */
DROP FUNCTION IF EXISTS test_add1(INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_select(INTEGER, INTEGER[]);
--/* BEGIN: recognise symbol "=>" */

--/* BEGIN: the create function, dispaly function, delete function */
--/* test without variadic parameter */
--/* prepare for test */
DROP FUNCTION IF EXISTS test_add1(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add2(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add3(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add4(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add5(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add6(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add7(INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add8(INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add9(INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add10(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add11(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_func1(TEXT, NUMERIC, CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func2(TEXT, NUMERIC, CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func3(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func4(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func5(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func6(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func7(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func8(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func9(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func10(CHAR, INTEGER);
\x
--/* test */
CREATE OR REPLACE FUNCTION test_add1(a IN INTEGER, b IN INTEGER, c IN INTEGER, d IN INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= a + b + c + d;
	RETURN temp;
END;
/
\df test_add1
CREATE OR REPLACE FUNCTION test_add2(a IN INTEGER default 1, b IN INTEGER default 2, c IN INTEGER default 3, d IN INTEGER default 4) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= a + b + c + d;
	RETURN temp;
END;
/
\df test_add2
CREATE OR REPLACE FUNCTION test_add3(a IN INTEGER, b IN INTEGER, c IN INTEGER default 3, d IN INTEGER default 4) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= a + b + c + d;
	RETURN temp;
END;
/
\df test_add3
CREATE OR REPLACE FUNCTION test_add4(a IN INTEGER default 1, b IN INTEGER, c IN INTEGER default 3, d IN INTEGER default 4) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= a + b + c + d;
	RETURN temp;
END;
/
\df test_add4
CREATE OR REPLACE FUNCTION test_add5(a IN INTEGER default 1, b IN INTEGER default 2, c IN INTEGER default 3, d IN INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= a + b + c + d;
	RETURN temp;
END;
/
\df test_add5
CREATE OR REPLACE FUNCTION test_add6(a IN INTEGER default 1, b IN INTEGER, c IN INTEGER, d IN INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= a + b + c + d;
	RETURN temp;
END;
/
\df test_add6
CREATE OR REPLACE FUNCTION test_add7(a IN INTEGER default 1, b IN INTEGER, c IN INTEGER, d OUT INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= a + b + c;
	d := temp;
END;
/
\df test_add7
CREATE OR REPLACE FUNCTION test_add8(a IN INTEGER default 1, b OUT INTEGER, c IN INTEGER DEFAULT 1, d IN INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= a + b + c;
	d := temp;
END;
/
\df test_add8
CREATE OR REPLACE FUNCTION test_add9(INTEGER default 1, OUT INTEGER, INTEGER DEFAULT 1, INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= $1 + $4 + $3;
	$2 := temp;
END;
/
\df test_add9
CREATE OR REPLACE FUNCTION test_add10(INTEGER default 1, INTEGER, INTEGER DEFAULT 1, INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= $1 + $2 + $4 + $3;
	RETURN temp;
END;
/
\df test_add10
CREATE OR REPLACE FUNCTION test_add11(INTEGER default 1, OUT INTEGER, INTEGER DEFAULT 1, OUT INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= $1 + $3;
	$2 := temp;
END;
/
\df test_add11
CREATE OR REPLACE FUNCTION test_func1(name IN TEXT, id IN NUMERIC, sex IN CHAR, age IN INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	IF sex <> 'f' THEN
		temp:=age;
	ELSE
		temp:= (-1)*age;
	END IF;
	RETURN temp;
END;
/
\df test_func1
CREATE OR REPLACE FUNCTION test_func2(name IN TEXT default 'dfm', id IN NUMERIC, sex IN CHAR default 'f', age IN INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	IF sex <> 'f' THEN
		temp:=age;
	ELSE
		temp:= (-1)*age;
	END IF;
	RETURN temp;
END;
/
\df test_func2
CREATE OR REPLACE FUNCTION test_func3(name OUT TEXT, id OUT NUMERIC, sex IN CHAR, age IN INTEGER) RETURN RECORD
AS
	temp INTEGER :=0;
	id_dafault1 NUMERIC := 053497;
	id_dafault2 NUMERIC := 053498;
	name_dafault1 TEXT := 'jyh';
	name_dafault2 TEXT := 'dfm';	
BEGIN
	IF sex <> 'f' THEN
		name:=name_dafault1;
	ELSE
		name:= name_dafault2;
	END IF;
	IF age < 20 THEN
		id:=id_dafault1;
	ELSE
		id:= id_dafault2;
	END IF;
END;
/
\df test_func3
CREATE OR REPLACE FUNCTION test_func4(id OUT NUMERIC, sex IN CHAR, name OUT TEXT, age IN INTEGER) RETURN RECORD
AS
	temp INTEGER :=0;
	id_dafault1 NUMERIC := 053497;
	id_dafault2 NUMERIC := 053498;
	name_dafault1 TEXT := 'jyh';
	name_dafault2 TEXT := 'dfm';	
BEGIN
	IF sex <> 'f' THEN
		name:=name_dafault1;
	ELSE
		name:= name_dafault2;
	END IF;
	IF age < 20 THEN
		id:=id_dafault1;
	ELSE
		id:= id_dafault2;
	END IF;
END;
/
\df test_func4
CREATE OR REPLACE FUNCTION test_func5(id OUT NUMERIC, sex IN CHAR, age IN INTEGER, name OUT TEXT) RETURN RECORD
AS
	temp INTEGER :=0;
	id_dafault1 NUMERIC := 053497;
	id_dafault2 NUMERIC := 053498;
	name_dafault1 TEXT := 'jyh';
	name_dafault2 TEXT := 'dfm';	
BEGIN
	IF sex <> 'f' THEN
		name:=name_dafault1;
	ELSE
		name:= name_dafault2;
	END IF;
	IF age < 20 THEN
		id:=id_dafault1;
	ELSE
		id:= id_dafault2;
	END IF;
END;
/
\df test_func5
CREATE OR REPLACE FUNCTION test_func6(sex IN CHAR, id OUT NUMERIC, age IN INTEGER, name OUT TEXT) RETURN RECORD
AS
	temp INTEGER :=0;
	id_dafault1 NUMERIC := 053497;
	id_dafault2 NUMERIC := 053498;
	name_dafault1 TEXT := 'jyh';
	name_dafault2 TEXT := 'dfm';	
BEGIN
	IF sex <> 'f' THEN
		name:=name_dafault1;
	ELSE
		name:= name_dafault2;
	END IF;
	IF age < 20 THEN
		id:=id_dafault1;
	ELSE
		id:= id_dafault2;
	END IF;
END;
/
\df test_func6
CREATE OR REPLACE FUNCTION test_func7(name OUT TEXT, id OUT NUMERIC, sex IN CHAR default 'f', age IN INTEGER) RETURN RECORD
AS
	temp INTEGER :=0;
	id_dafault1 NUMERIC := 053497;
	id_dafault2 NUMERIC := 053498;
	name_dafault1 TEXT := 'jyh';
	name_dafault2 TEXT := 'dfm';	
BEGIN
	IF sex <> 'f' THEN
		name:=name_dafault1;
	ELSE
		name:= name_dafault2;
	END IF;
	IF age < 20 THEN
		id:=id_dafault1;
	ELSE
		id:= id_dafault2;
	END IF;
END;
/
\df test_func7
CREATE OR REPLACE FUNCTION test_func8(id OUT NUMERIC, sex IN CHAR, name OUT TEXT, age IN INTEGER default 17) RETURN RECORD
AS
	temp INTEGER :=0;
	id_dafault1 NUMERIC := 053497;
	id_dafault2 NUMERIC := 053498;
	name_dafault1 TEXT := 'jyh';
	name_dafault2 TEXT := 'dfm';	
BEGIN
	IF sex <> 'f' THEN
		name:=name_dafault1;
	ELSE
		name:= name_dafault2;
	END IF;
	IF age < 20 THEN
		id:=id_dafault1;
	ELSE
		id:= id_dafault2;
	END IF;
END;
/
\df test_func8
CREATE OR REPLACE FUNCTION test_func9(id OUT NUMERIC, sex IN CHAR default 'f', age IN INTEGER, name OUT TEXT) RETURN RECORD
AS
	temp INTEGER :=0;
	id_dafault1 NUMERIC := 053497;
	id_dafault2 NUMERIC := 053498;
	name_dafault1 TEXT := 'jyh';
	name_dafault2 TEXT := 'dfm';	
BEGIN
	IF sex <> 'f' THEN
		name:=name_dafault1;
	ELSE
		name:= name_dafault2;
	END IF;
	IF age < 20 THEN
		id:=id_dafault1;
	ELSE
		id:= id_dafault2;
	END IF;
END;
/
\df test_func9
CREATE OR REPLACE FUNCTION test_func10(sex IN CHAR, id OUT NUMERIC, age IN INTEGER default 18, name OUT TEXT) RETURN RECORD
AS
	temp INTEGER :=0;
	id_dafault1 NUMERIC := 053497;
	id_dafault2 NUMERIC := 053498;
	name_dafault1 TEXT := 'jyh';
	name_dafault2 TEXT := 'dfm';	
BEGIN
	IF sex <> 'f' THEN
		name:=name_dafault1;
	ELSE
		name:= name_dafault2;
	END IF;
	IF age < 20 THEN
		id:=id_dafault1;
	ELSE
		id:= id_dafault2;
	END IF;
END;
/
\df test_func10
--/* clean up */
DROP FUNCTION IF EXISTS test_add1(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add2(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add3(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add4(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add5(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add6(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add7(INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add8(INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add9(INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add10(INTEGER, INTEGER, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_add11(INTEGER, INTEGER);
DROP FUNCTION IF EXISTS test_func1(TEXT, NUMERIC, CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func2(TEXT, NUMERIC, CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func3(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func4(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func5(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func6(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func7(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func8(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func9(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func10(CHAR, INTEGER);
\x
--/* test with variadic parameter */
--/* prepare for test */
DROP FUNCTION IF EXISTS test_select1(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select2(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select3(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select4(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select5(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select6(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select7(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select8(INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select9(INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select10(INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select11(INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select12(INTEGER, INTEGER, INTEGER, INTEGER[]);
\x
--/* test */
CREATE OR REPLACE FUNCTION test_select1(a IN INTEGER, b IN INTEGER, c IN INTEGER, d IN INTEGER, VARIADIC arr INTEGER[]) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	if (temp > a) THEN
		temp := a;
	END IF;
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > c) THEN
		temp := c;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	RETURN temp;
END;
/
\df test_select1
CREATE OR REPLACE FUNCTION test_select2(a IN INTEGER DEFAULT 1, b IN INTEGER DEFAULT 2, c IN INTEGER DEFAULT 3, d IN INTEGER DEFAULT 4, VARIADIC arr INTEGER[] DEFAULT ARRAY[-1, -2, -3, -4]) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	if (temp > a) THEN
		temp := a;
	END IF;
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > c) THEN
		temp := c;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	RETURN temp;
END;
/
\df test_select2
CREATE OR REPLACE FUNCTION test_select3(a IN INTEGER, b IN INTEGER, c IN INTEGER DEFAULT 3, d IN INTEGER DEFAULT 4, VARIADIC arr INTEGER[] DEFAULT ARRAY[-1, -2, -3, -4]) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	if (temp > a) THEN
		temp := a;
	END IF;
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > c) THEN
		temp := c;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	RETURN temp;
END;
/
\df test_select3
CREATE OR REPLACE FUNCTION test_select4(a IN INTEGER, b IN INTEGER, c IN INTEGER, d IN INTEGER, VARIADIC arr INTEGER[] DEFAULT ARRAY[-1, -2, -3, -4]) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	if (temp > a) THEN
		temp := a;
	END IF;
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > c) THEN
		temp := c;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	RETURN temp;
END;
/
\df test_select4
CREATE OR REPLACE FUNCTION test_select5(a IN INTEGER DEFAULT 1, b IN INTEGER, c IN INTEGER DEFAULT 3, d IN INTEGER DEFAULT 4, VARIADIC arr INTEGER[] DEFAULT ARRAY[-1, -2, -3, -4]) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	if (temp > a) THEN
		temp := a;
	END IF;
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > c) THEN
		temp := c;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	RETURN temp;
END;
/
\df test_select5
CREATE OR REPLACE FUNCTION test_select6(a IN INTEGER DEFAULT 1, b IN INTEGER DEFAULT 2, c IN INTEGER DEFAULT 3, d IN INTEGER, VARIADIC arr INTEGER[]) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	if (temp > a) THEN
		temp := a;
	END IF;
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > c) THEN
		temp := c;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	RETURN temp;
END;
/
\df test_select6
CREATE OR REPLACE FUNCTION test_select7(a IN INTEGER DEFAULT 1, b IN INTEGER, c IN INTEGER, d IN INTEGER, VARIADIC arr INTEGER[]) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	if (temp > a) THEN
		temp := a;
	END IF;
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > c) THEN
		temp := c;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	RETURN temp;
END;
/
\df test_select7
CREATE OR REPLACE FUNCTION test_select8(a IN INTEGER, b IN INTEGER, c OUT INTEGER, d IN INTEGER, VARIADIC arr INTEGER[]) RETURN INTEGER
AS
	temp INTEGER :=1;
BEGIN
	if (temp > a) THEN
		temp := a;
	END IF;
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	c := temp;
END;
/
\df test_select8
CREATE OR REPLACE FUNCTION test_select9(a IN INTEGER DEFAULT 1, b IN INTEGER DEFAULT 2, c OUT INTEGER, d IN INTEGER DEFAULT 4, VARIADIC arr INTEGER[] DEFAULT ARRAY[-1, -2, -3, -4]) RETURN INTEGER
AS
	temp INTEGER :=1;
BEGIN
	if (temp > a) THEN
		temp := a;
	END IF;
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	c := temp;
END;
/
\df test_select9
CREATE OR REPLACE FUNCTION test_select10(c IN INTEGER DEFAULT 3, a OUT INTEGER, d IN INTEGER, VARIADIC arr INTEGER[] DEFAULT ARRAY[-1, -2, -3, -4]) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	if (temp > c) THEN
		temp := c;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	a := temp;
END;
/
\df test_select10
CREATE OR REPLACE FUNCTION test_select11(a OUT INTEGER, b IN INTEGER DEFAULT 2, d IN INTEGER DEFAULT 4, VARIADIC arr INTEGER[] DEFAULT ARRAY[-1, -2, -3, -4]) RETURN INTEGER
AS
	temp INTEGER :=1;
BEGIN
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	a := temp;
END;
/
\df test_select11
CREATE OR REPLACE FUNCTION test_select12(b IN INTEGER, c IN INTEGER, d IN INTEGER, VARIADIC arr INTEGER[] DEFAULT ARRAY[-1, -2, -3, -4], a OUT INTEGER) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	if (temp > b) THEN
		temp := b;
	END IF;
	if (temp > c) THEN
		temp := c;
	END IF;
	if (temp > d) THEN
		temp := d;
	END IF;
	temp := arr[temp];
	a := temp;
END;
/
\df test_select12
CREATE OR REPLACE FUNCTION test_func1(name OUT TEXT, id OUT NUMERIC, sex IN CHAR, age IN INTEGER, VARIADIC arr INTEGER[]) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
\df test_func1
CREATE OR REPLACE FUNCTION test_func2(name OUT TEXT, sex IN CHAR, age IN INTEGER, VARIADIC arr INTEGER[], id OUT NUMERIC) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
\df test_func2
CREATE OR REPLACE FUNCTION test_func3(sex IN CHAR, name OUT TEXT, age IN INTEGER, VARIADIC arr INTEGER[], id OUT NUMERIC) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
\df test_func3
CREATE OR REPLACE FUNCTION test_func4(sex IN CHAR, age IN INTEGER, VARIADIC arr INTEGER[], name OUT TEXT, id OUT NUMERIC) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
\df test_func4
CREATE OR REPLACE FUNCTION test_func5(sex IN CHAR, age IN INTEGER default 20, VARIADIC arr INTEGER[], name OUT TEXT, id OUT NUMERIC) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
\df test_func5
CREATE OR REPLACE FUNCTION test_func6(sex IN CHAR default 'f', age IN INTEGER default 20, VARIADIC arr INTEGER[], name OUT TEXT, id OUT NUMERIC) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
\df test_func6
CREATE OR REPLACE FUNCTION test_func7(sex IN CHAR, name OUT TEXT, age IN INTEGER, VARIADIC arr INTEGER[] default ARRAY[1, 2, 3, 4], id OUT NUMERIC) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
\df test_func7
CREATE OR REPLACE FUNCTION test_func8(sex IN CHAR default 'f', name OUT TEXT, age IN INTEGER, VARIADIC arr INTEGER[] default ARRAY[1, 2, 3, 4], id OUT NUMERIC) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
\df test_func8
CREATE OR REPLACE FUNCTION test_func9(sex IN CHAR default 'f', name OUT TEXT, age IN INTEGER default 20, VARIADIC arr INTEGER[] default ARRAY[1, 2, 3, 4], id OUT NUMERIC) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
\df test_func9
CREATE OR REPLACE FUNCTION test_func10(sex IN CHAR, name OUT TEXT, age IN INTEGER default 20, VARIADIC arr INTEGER[] default ARRAY[1, 2, 3, 4], id OUT NUMERIC) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
\df test_func10
--/* clean up */
DROP FUNCTION IF EXISTS test_select1(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select2(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select3(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select4(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select5(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select6(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select7(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select8(INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select9(INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select10(INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select11(INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_select12(INTEGER, INTEGER, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func1(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func2(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func3(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func4(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func5(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func6(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func7(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func8(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func9(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func10(CHAR, INTEGER, INTEGER[]);
\x
--/* BEGIN: the create function, dispaly function, delete function */

--/* BEGIN: invoke function directly */
--/* prepare for test */
DROP FUNCTION IF EXISTS test_func1(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func2(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func3(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func4(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func5(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func6(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func7(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func8(CHAR, INTEGER, INTEGER[]);
--/* test */
--/* without default argument */
CREATE OR REPLACE FUNCTION test_func1(id OUT NUMERIC, sex IN CHAR, name OUT TEXT, age IN INTEGER) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	IF temp_sex > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_sex > 1 THEN
		name := name_dafault2;
	END IF;
END;
/
SELECT test_func1('f',20);
SELECT test_func1(age:=20,sex=>'f');
SELECT test_func1('f',age:=20);
--/* with default argument */
CREATE OR REPLACE FUNCTION test_func1(id OUT NUMERIC, sex IN CHAR, name OUT TEXT, age IN INTEGER) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	IF temp_sex > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_sex > 1 THEN
		name := name_dafault2;
	END IF;
END;
/
SELECT test_func1('f',20);
SELECT test_func1(age:=20,sex=>'f');
SELECT test_func1('f',age:=20);
CREATE OR REPLACE FUNCTION test_func2(id OUT NUMERIC, sex IN CHAR default 'f', name OUT TEXT, age IN INTEGER) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	IF temp_sex > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_sex > 1 THEN
		name := name_dafault2;
	END IF;
END;
/
SELECT test_func2('f',20);
SELECT test_func2(age:=20,sex=>'f');
SELECT test_func2('f',age:=20);
SELECT test_func2(age:=22);
SELECT test_func2(age:=22);
CREATE OR REPLACE FUNCTION test_func3(id OUT NUMERIC, sex IN CHAR, name OUT TEXT, age IN INTEGER default 20) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	IF temp_sex > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_sex > 1 THEN
		name := name_dafault2;
	END IF;
END;
/
SELECT test_func3('f',20);
SELECT test_func3(age:=20,sex=>'f');
SELECT test_func3('f',age:=20);
SELECT test_func3('f');
SELECT test_func3(sex:='f');
CREATE OR REPLACE FUNCTION test_func4(id OUT NUMERIC, sex IN CHAR default 'f', name OUT TEXT, age IN INTEGER default 20) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	IF temp_sex > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_sex > 1 THEN
		name := name_dafault2;
	END IF;
END;
/
SELECT test_func4('f',20);
SELECT test_func4(age:=20,sex=>'f');
SELECT test_func4('f',age:=20);
SELECT test_func4('f');
SELECT test_func4(sex:='f');
SELECT test_func4(age:= 20);
SELECT test_func4();
CREATE OR REPLACE FUNCTION test_func5(id OUT NUMERIC, sex IN CHAR, name OUT TEXT, age IN INTEGER, VARIADIC arr INTEGER[]) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
SELECT test_func5('f',20,20,21);
SELECT test_func5('f',age:=24,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func5('f',24,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func5(sex:='m',age:=25,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func5(age:=25,sex:='m',VARIADIC arr:=ARRAY[2,3]);
CREATE OR REPLACE FUNCTION test_func6(id OUT NUMERIC, sex IN CHAR default 'f', name OUT TEXT, age IN INTEGER, VARIADIC arr INTEGER[]) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
SELECT test_func6('f',20,20,21);
SELECT test_func6('f',age:=24,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func6('f',24,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func6(sex:='m',age:=25,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func6(age:=25,sex:='m',VARIADIC arr:=ARRAY[2,3]);
--fail in bs: We disallow VARIADIC with named arguments unless the last argument actually matched the variadic parameter
SELECT test_func6(age:=25,VARIADIC arr:=ARRAY[2,3]);
CREATE OR REPLACE FUNCTION test_func7(id OUT NUMERIC, sex IN CHAR default 'f', name OUT TEXT, age IN INTEGER, VARIADIC arr INTEGER[] default ARRAY[1,2,3,4]) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
SELECT test_func7('f',20,20,21);
SELECT test_func7('f',age:=24,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func7('f',24,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func7(sex:='m',age:=25,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func7(age:=25,sex:='m',VARIADIC arr:=ARRAY[2,3]);
SELECT test_func7('f',20);
--fail in bs: Named or mixed notation can match a variadic function only if expand_variadic is off
SELECT test_func7('f',age:=20);
SELECT test_func7(age:=8);
CREATE OR REPLACE FUNCTION test_func8(id OUT NUMERIC, sex IN CHAR default 'f', name OUT TEXT, age IN INTEGER default 20, VARIADIC arr INTEGER[] default ARRAY[1,2,3,4]) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';
	name_dafault3 TEXT := 'jyh';
	name_dafault4 TEXT := 'fml';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 3;
	id_dafault4 NUMERIC := 4;
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := arr[temp_sex];
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
	IF temp_num > 2 THEN
		name := name_dafault3;
	END IF;
	IF temp_num > 3 THEN
		name := name_dafault4;
	END IF;	
END;
/
SELECT test_func8('f',20,20,21);
SELECT test_func8('f',age:=24,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func8('f',24,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func8(sex:='m',age:=25,VARIADIC arr:=ARRAY[2,3]);
SELECT test_func8(age:=25,sex:='m',VARIADIC arr:=ARRAY[2,3]);
SELECT test_func8('f',20);
SELECT test_func8();
SELECT test_func8('f');
--fail in bs: Named or mixed notation can match a variadic function only if expand_variadic is off
SELECT test_func8('f',age:=20);
SELECT test_func8(age:=8);
--/* clean up */
DROP FUNCTION IF EXISTS test_func1(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func2(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func3(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func4(CHAR, INTEGER);
DROP FUNCTION IF EXISTS test_func5(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func6(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func7(CHAR, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS test_func8(CHAR, INTEGER, INTEGER[]);
--/* BEGIN: invoke function directly */

--/* BEGIN: invoke a function innner a function */
--/* prepare for test */
DROP FUNCTION IF EXISTS func_inner(CHAR, INTEGER);
DROP FUNCTION IF EXISTS func_outter(CHAR, INTEGER);
--/* test */
CREATE OR REPLACE FUNCTION func_inner(id OUT NUMERIC, sex IN CHAR default 'f', name OUT TEXT, age IN INTEGER default 20) RETURN RECORD
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 TEXT := 'chao';
	name_dafault2 TEXT := 'dfm';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
	id_dafault3 NUMERIC := 1;
	id_dafault4 NUMERIC := 2;	
BEGIN
	temp_age :=  age%4 + 1;
	IF temp_age > 0 THEN
		id := id_dafault1;
	END IF;
	IF temp_age > 1 THEN
		id := id_dafault2;
	END IF;
	IF temp_age > 2 THEN
		id := id_dafault3;
	END IF;
	IF temp_age > 3 THEN
		id := id_dafault4;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := temp_sex;
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
END;
/
CREATE OR REPLACE FUNCTION func_outter(id OUT NUMERIC, sex IN CHAR default 'f', name OUT TEXT, age IN INTEGER default 20) RETURN RECORD
AS
	temp_id NUMERIC := 00000;
	temp_sex CHAR := 'm';
	temp_name text := 'dfm_chao';
	temp_age NUMERIC := 18;	
BEGIN
		temp_sex := sex;
		temp_age := age;
		func_inner(temp_id, temp_sex, temp_name, temp_age);
		id := temp_id;
		name := temp_name;
END;
/
select func_outter();
select func_outter('m');
select func_outter('f',19);
select func_outter('m',age=>20);
select func_outter(sex:='f',age=>21);
select func_outter(age=>21,sex:='f');
--/* clean up */
DROP FUNCTION IF EXISTS func_inner(CHAR, INTEGER);
DROP FUNCTION IF EXISTS func_outter(CHAR, INTEGER);
--/* BEGIN: invoke a function innner a function */
--/ * invoke a function with inout parameter and without parametername */
CREATE OR REPLACE FUNCTION test_add(IN INTEGER default 1,INOUT INTEGER,IN INTEGER default 3,IN INTEGER default 4) RETURN INTEGER
AS
	temp INTEGER :=0;
BEGIN
	temp:= $1 + $2 + $3 + $4;
	$2 = temp;
END;
/
\x
\df test_add
SELECT test_add(1,1);
SELECT test_add(1,1,1);
SELECT test_add(1,1,1,1);
DROP FUNCTION test_add(INTEGER,INTEGER,INTEGER,INTEGER);
/* create a function without in parameter */
CREATE OR REPLACE FUNCTION test_print(OUT INTEGER) RETURN INTEGER
AS
	temp INTEGER :=10;
BEGIN
	$1 = temp;
END;
/
\df test_print
\x
SELECT test_print();
DROP FUNCTION test_print();
--/* prepare for test */
DROP PROCEDURE func_inner;
DROP PROCEDURE func_outter1;
DROP PROCEDURE func_outter2;
DROP PROCEDURE func_outter3;
DROP PROCEDURE func_outter4;
DROP PROCEDURE func_outter5;
DROP PROCEDURE func_outter6;
DROP PROCEDURE func_outter7;
DROP PROCEDURE func_outter8;
CREATE OR REPLACE PROCEDURE func_inner(id OUT NUMERIC, sex IN CHAR default 'f', name OUT varchar2, age IN INTEGER default 20)
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 varchar2(100) := 'chao';
	name_dafault2 varchar2(100) := 'dfm';	
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;	
BEGIN
	temp_age :=  age;
	id := id_dafault1;
	IF temp_age > 40 THEN
		id := id_dafault2;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := temp_sex;
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
END;
/

CALL func_inner(id,name=>'dfm');
CALL func_inner(id=>0,name=>'dfm');
CALL func_inner(id, 'f',name=>'dfm');
CALL func_inner(id, sex=>'f',name=>'dfm');
CALL func_inner(id, age=>80,name=>'dfm');
CALL func_inner(id, age=>80, sex=>'m', name=>'dfm');
CALL func_inner(id, sex=>'m',age=>80, name=>'dfm');
CALL func_inner(id, name=>'dfm',sex=>'m',age=>20);
CALL func_inner(id,name:='dfm');
CALL func_inner(id:=0,name:='dfm');
CALL func_inner(id, 'f',name:='dfm');
CALL func_inner(id, sex:='f',name:='dfm');
CALL func_inner(id, age:=80,name:='dfm');
CALL func_inner(id, age:=80, sex:='m', name:='dfm');
CALL func_inner(id, sex:='m',age:=80, name:='dfm');
CALL func_inner(id, name:='dfm',sex=>'m',age:=20);
--fail for output parameter "name" not assigned
CALL func_inner(id, sex=>'f',age=>40);
CALL func_inner(id, sex=>'f',id=>40);
CALL func_inner(id, sex=>'f',cd=>40);
CALL func_inner(id, sex=>'f');

CREATE OR REPLACE PROCEDURE func_outter1(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,temp_sex,name=>temp_name,age=>temp_age);
END;
/
CALL func_outter1();
CALL func_outter1('f');
CALL func_outter1('f', 20);
CALL func_outter1(age=>20);
CALL func_outter1(age=>20, sex=>'m');

CREATE OR REPLACE PROCEDURE func_outter2(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(name=>temp_name,id=>temp_id);
	DBMS_OUTPUT.PUT_LINE(temp_name);	
END;
/
CALL func_outter2();
CALL func_outter2('f');
CALL func_outter2('f', 20);
CALL func_outter2(age=>20);
CALL func_outter2(age=>20, sex=>'m');

CREATE OR REPLACE PROCEDURE func_outter3(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,name=>temp_name);
END;
/
CALL func_outter3();
CALL func_outter3('f');
CALL func_outter3('f', 20);
CALL func_outter3(age=>20);
CALL func_outter3(age=>20, sex=>'m');

CREATE OR REPLACE PROCEDURE func_outter4(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,temp_sex,name=>temp_name);
END;
/
CALL func_outter4();
CALL func_outter4('f');
CALL func_outter4('f', 20);
CALL func_outter4(age=>20);
CALL func_outter4(age=>20, sex=>'m');

CREATE OR REPLACE PROCEDURE func_outter5(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,age=>temp_age,name=>temp_name);
END;
/
CALL func_outter5();
CALL func_outter5('f');
CALL func_outter5('f', 20);
CALL func_outter5(age=>20);
CALL func_outter5(age=>20, sex=>'m');

CREATE OR REPLACE PROCEDURE func_outter6(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(sex=>temp_sex,id=>temp_id,age=>temp_age,name=>temp_name);
END;
/
CALL func_outter6();
CALL func_outter6('f');
CALL func_outter6('f', 20);
CALL func_outter6(age=>20);
CALL func_outter6(age=>20, sex=>'m');

CREATE OR REPLACE PROCEDURE func_outter7(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,temp_sex,temp_name,temp_age);
END;
/
CALL func_outter7();
CALL func_outter7('f');
CALL func_outter7('f', 20);
CALL func_outter7(age=>20);
CALL func_outter7(age=>20, sex=>'m');

CREATE OR REPLACE PROCEDURE func_outter8(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,age=>temp_age,sex=>temp_sex,name=>temp_name);
END;
/
CALL func_outter8();
CALL func_outter8('f');
CALL func_outter8('f', 20);
CALL func_outter8(age=>20);
CALL func_outter8(age=>20, sex=>'m');

CREATE OR REPLACE PROCEDURE test_default_out( i in integer, j out integer, k out integer, m in integer default 1, n in integer default 1, o out integer) 
AS
	BEGIN
		j:=i;
		k := j + i;
		o := i + j + k + m + n;
		RETURN;
    END;
/

declare
a int := 1;
b int := 1;
c int := 1;
begin
	test_default_out(1, k=>a, n=>1, j=>b, o=>c);
end;
/
CREATE OR REPLACE PROCEDURE SP_TESP
(
    v1 INTEGER,
    v2 VARCHAR2,
    v3 INTEGER
)
AS
BEGIN
END;
/

CALL SP_TESP(Null,'JOE');
CALL SP_TESP(Null,'JOE',123);
CALL SP_TESP(Null,'JOE',123,1);
--/* clean up */
DROP PROCEDURE func_inner;
DROP PROCEDURE func_outter1;
DROP PROCEDURE func_outter2;
DROP PROCEDURE func_outter3;
DROP PROCEDURE func_outter4;
DROP PROCEDURE func_outter5;
DROP PROCEDURE func_outter6;
DROP PROCEDURE func_outter7;
DROP PROCEDURE func_outter8;
DROP PROCEDURE test_default_out;
DROP PROCEDURE SP_TESP;
\c regression;
drop database IF EXISTS pl_test_func_default;
