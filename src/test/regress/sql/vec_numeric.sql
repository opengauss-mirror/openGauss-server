---- prepare work
DROP SCHEMA IF EXISTS vec_numeric_to_bigintger CASCADE;
CREATE SCHEMA vec_numeric_to_bigintger;
SET current_schema = vec_numeric_to_bigintger;
SET enable_fast_numeric = on;

---- NUMERIC IS NOT DISTRIBUTE KEY
CREATE TABLE vec_numeric_1 (v_id int, v_numeric_1 numeric(15, 2), v_numeric_2 numeric(10, 5)) WITH (orientation=column);
COPY vec_numeric_1 FROM STDIN;
1	-1.1	-111.1
2	-2.2	-222.2
3	3	-333.3
4	4	-444.4
5	5	-555.5
6	6	-666.6
7	7	-111.1
8	8	
9		
\.
analyze vec_numeric_1 (v_numeric_1, v_numeric_2);

---- unary operator: +(uplus), -(uminus), @(abs)
SELECT v_id, v_numeric_1, v_numeric_2, +v_numeric_1 as uplus, -v_numeric_2 as uminus, abs(v_numeric_1), abs(v_numeric_2), @v_numeric_1 as abs1, @v_numeric_2 as abs2 FROM vec_numeric_1 ORDER BY v_numeric_1, v_numeric_2;

---- unary operator: !(!!fac), |/(sqrt), ||/dcbrt
SELECT v_id, v_numeric_1, v_numeric_2, CAST(v_numeric_1 AS int)! as val1, CAST(v_numeric_2 AS int)! as val2, |/abs(v_numeric_2) as val3, ||/abs(v_numeric_2) as val4 FROM vec_numeric_1 ORDER BY v_numeric_1, v_numeric_2;

---- cast const numeric data to big integer(int64/int128)
SELECT v_id, v_numeric_1, v_numeric_2, 999999999999999999 + v_numeric_1, 999999999.999999999 + v_numeric_2, v_numeric_2 + 0.999999999999999999 FROM vec_numeric_1 ORDER BY 2, 3;
SELECT v_id, v_numeric_1, v_numeric_2, 0.11111111111111111 * v_numeric_1, 999999999.999999999 * v_numeric_2, v_numeric_2 * 0.999999999999999999 FROM vec_numeric_1 ORDER BY 2, 3;

---- NUMERIC TABLE
CREATE TABLE vec_numeric_2(id int, val1 numeric(19,2), val2 numeric(19,4)) WITH (orientation=column);
COPY vec_numeric_2 FROM STDIN;
1	8.88	8.8888
2	-8.88	-8.8888
1	88.88	88.8888
2	-88.88	-88.8888
1	888.88	888.8888
2	-888.88	-888.8888
1	8888.88	8888.8888
2	-8888.88	-8888.8888
1	88888.88	88888.8888
2	-88888.88	-88888.8888
1	888888.88	888888.8888
2	-888888.88	-888888.8888
1	888888888888.88	8888888888.8888
2	-888888888888.88	-8888888888.8888
3	8888888888888.88	88888888888.8888
4	-8888888888888.88	-88888888888.8888
5	88888888888888.88	888888888888.8888
6	-88888888888888.88	-888888888888.8888
7	888888888888888.88	8888888888888.8888
8	-888888888888888.88	-8888888888888.8888
9	888888888888888.88	88888888888888.8888
10	-8888888888888888.88	-88888888888888.8888
11	88888888888888888.88	888888888888888.8888
12	-888888888888888.88	-888888888888888.8888
1	8.88	8.8888
1	-8.88	-8.8888
1	88.88	88.8888
1	-88.88	-88.8888
1	888.88	888.8888
1	-888.88	-888.8888
1	8888.88	8888.8888
1	-8888.88	-8888.8888
1	88888.88	88888.8888
1	-88888.88	-88888.8888
1	888888.88	888888.8888
1	-888888.88	-888888.8888
1	888888888888.88	8888888888.8888
1	-888888888888.88	-8888888888.8888
1	8888888888888.88	88888888888.8888
1	-8888888888888.88	-88888888888.8888
1	88888888888888.88	888888888888.8888
1	-88888888888888.88	-888888888888.8888
1	888888888888888.88	8888888888888.8888
1	-888888888888888.88	-8888888888888.8888
1	888888888888888.88	88888888888888.8888
1	-8888888888888888.88	-88888888888888.8888
1	88888888888888888.88	888888888888888.8888
1	-888888888888888.88	-888888888888888.8888
\.
analyze vec_numeric_2;

---- bi64add64
SELECT id, val1 + 1.0, 1.0 + val1, val1 * 10.0 + 10.0 * val1, val2 + val1 * 10, val1 * 10 + val2 FROM vec_numeric_2 ORDER BY 1,2,3,4;

---- bi64sub64
SELECT id, val1 - 1.0, 1.0 - val1, val1 * 10.0 - (-10.0) * val1, val2 - val1 * (-10), val1 * 10 - val2 * (-1) FROM vec_numeric_2 ORDER BY 1,2,3,4;

---- bi64mul64
SELECT id, val1 * 10.0, 1.0 * val2, val1 * val2 FROM vec_numeric_2 ORDER BY 1,2,3,4;

---- bi128add128
SELECT id, val1 * val1  + val1 * val1, val1 + val1 * val1, val1 * val1 + val1 FROM vec_numeric_2 ORDER BY 1,2;
SELECT id, val1 * 10000000000000000000  + 10000000000000000000.00 * val1, val1 * 10000000000000000000 + 1.0000000000000000000 * val1, 1.0000000000000000000 * val1 + val1 FROM vec_numeric_2 ORDER BY 1,2;

---- bi128sub128
SELECT id, val1 * val1 - (-1) * val1 * val1, val1 - (-1) * val1 * val1, val1 * val1 - (-1) * val1 FROM vec_numeric_2 ORDER BY 1,2;
SELECT id, val1 * 10000000000000000000  - -10000000000000000000.00 * val1, val1 * 10000000000000000000 - -1.0000000000000000000 * val1, 1.0000000000000000000 * val1 - (-1) * val1 FROM vec_numeric_2 ORDER BY 1,2;

---- bi128mul128
SELECT id, val1 * 10000000000000000 * val2, 1.00000000000000000000 * val1 * val2, (val1 * 100.00000000000000000000) * (10.9999999999999999999 * val2)  FROM vec_numeric_2 ORDER BY 1,2,3,4;

---- bi64cmp64 equal =
SELECT id, val1, val1 = 0.0000000001, val1 * 1.00 = val1 * 1, val1 * 1.000 = val1 * 100, val1 = 1.0000000000 * val1, val1 * 100000000.0000 * 0 = val1 * 0.00, val1 * 0.00 = val1 * 100000000.0000 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- bi64cmp64 not equal !=
SELECT id, val1, val1 != 0.0000000001, val1 * 1.00 != val1 * 1, val1 * 1.000 != val1 * 100, val1 != 1.0000000000 * val1, val1 * 100000000.0000 * 0 != val1 * 0.00, val1 * 0.00 != val1 * 100000000.0000 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- bi64cmp64 less and equal <=
SELECT id, val1, val1 <= 0.0000000001, val1 * 1.00 <= val1 * 1, val1 * 1.000 <= val1 * 100, val1 <= 1.0000000000 * val1, val1 * 100000000.0000 * 0 <= val1 * 0.00, val1 * 0.00 <= val1 * 100000000.0000 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- bi64cmp64 less than <
SELECT id, val1, val1 < 0.0000000001, val1 * 1.00 < val1 * 1, val1 * 1.000 < val1 * -100, val1 < -1.0000000000 * val1, val1 * 100000000.0000 * 0 < val1 * 0.00, val1 * -0.00 < val1 * 100000000.0000 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- bi64cmp64 great equal >=
SELECT id, val1, val1 >= 0.0000000001, val1 * 1.00 >= val1 * 1, val1 * 1.000 >= val1 * -100, val1 >= -1.0000000000 * val1, val1 * 100000000.0000 * 0 >= val1 * 0.00, val1 * -0.00 >= val1 * 100000000.0000 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- bi64cmp64 great than >
SELECT id, val1, val1 > 0.0000000001, val1 * 1.00 > val1 * 1, val1 * 1.000 > val1 * -100, val1 > -1.0000000000 * val1, val1 * 100000000.0000 * 0 > val1 * 0.00, val1 * -0.00 > val1 * 100000000.0000 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- bi128cmp128 equal =
SELECT id, val1, val1 * 1.00000000000000000 = val1, val1 * val2 + val1 = val1 * (val2 + 1), val1 = val1 * 1.0000000000000000000000, val1 * 1000000000000000000 = 1.000000000000000000 * val1, val1 * 1.00000000000000000 = val1 * 100000000000000000000000, val1 * val2 * val2 * 0.00 = val2 * 100000000.0000 * -99999999999 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;


---- bi128cmp128 not equal !=
SELECT id, val1, val1 * 1.00000000000000000 != val1, val1 * val2 + val1 != val1 * (val2 + 1), val1 != val1 * 1.0000000000000000000000, val1 * 1000000000000000000 != 1.000000000000000000 * val1, val1 * 1.00000000000000000 != val1 * 100000000000000000000000, val1 * val2 * val2 * 0.00 != val2 * 100000000.0000 * -99999999999 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- bi128cmp128 less and equal <=
SELECT id, val1, val1 * 1.00000000000000000 <= val1, val1 * val2 + val1 <= val1 * (val2 + 1), val1 <= val1 * 1.0000000000000000000000, val1 * 1000000000000000000 <= 1.000000000000000000 * val1, val1 * 1.00000000000000000 <= val1 * 100000000000000000000000, val1 * val2 * val2 * 0.00 <= val2 * 100000000.0000 * -99999999999 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- bi128cmp128 less than <
SELECT id, val1, val1 * 1.00000000000000000 < val1, val1 * val2 + val1 < val1 * (val2 + 1), val1 < val1 * 1.0000000000000000000000, val1 * 1000000000000000000 < 1.000000000000000000 * val1, val1 * 1.00000000000000000 < val1 * 100000000000000000000000, val1 * val2 * val2 * 0.00 < val2 * 100000000.0000 * -99999999999 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- bi128cmp128 large and equal >=
SELECT id, val1, val1 * 1.00000000000000000 >= val1, val1 * val2 + val1 >= val1 * (val2 + 1), val1 >= val1 * 1.0000000000000000000000, val1 * 1000000000000000000 >= 1.000000000000000000 * val1, val1 * 1.00000000000000000 >= val1 * 100000000000000000000000, val1 * val2 * val2 * 0.00 >= val2 * 100000000.0000 * -99999999999 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- bi128cmp128 large than >
SELECT id, val1, val1 * 1.00000000000000000 > val1, val1 * val2 + val1 > val1 * (val2 + 1), val1 > val1 * 1.0000000000000000000000, val1 * 1000000000000000000 > 1.000000000000000000 * val1, val1 * 1.00000000000000000 > val1 * 100000000000000000000000, val1 * val2 * val2 * 0.00 > val2 * 100000000.0000 * -99999999999 * 0 FROM vec_numeric_2 ORDER BY 1,2,3,4,5,6,7,8;

---- DROP SCHEMA
DROP SCHEMA vec_numeric_to_bigintger CASCADE;
