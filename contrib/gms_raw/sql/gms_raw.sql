create extension gms_raw;
create schema gms_raw_test;
set search_path=gms_raw_test;

-- bit_and
select gms_raw.bit_and('10','01');
select gms_raw.bit_and('01','10');
select gms_raw.bit_and('1010','01');
select gms_raw.bit_and('01','1010');
select gms_raw.bit_and('01000111','101011');
select gms_raw.bit_and(null,'01');
select gms_raw.bit_and('','01');
select gms_raw.bit_and('01',null);
select gms_raw.bit_and('01','');
select gms_raw.bit_and(null,null);
select gms_raw.bit_and('','');

select gms_raw.bit_and('0123456789','0123456789');
select gms_raw.bit_and('012','0123456');
select gms_raw.bit_and('012abc','0123456defg');

select gms_raw.bit_and('abcdefghijklmnopqrst','abcdefghijklmnopqrst');
select gms_raw.bit_and('我爱你中国','我爱你中国');
select gms_raw.bit_and('10', time'01:00:00');
select gms_raw.bit_and('10', '01:00');
select gms_raw.bit_and();
select gms_raw.bit_and('10');
select gms_raw.bit_and('10','10','10');

-- bit_or
select gms_raw.bit_or('10','01');
select gms_raw.bit_or('01','10');
select gms_raw.bit_or('1010','01');
select gms_raw.bit_or('01','1010');
select gms_raw.bit_or('01000111','101011');
select gms_raw.bit_or(null,'01');
select gms_raw.bit_or('','01');
select gms_raw.bit_or('01',null);
select gms_raw.bit_or('01','');
select gms_raw.bit_or(null,null);
select gms_raw.bit_or('','');

select gms_raw.bit_or('0123456789','0123456789');
select gms_raw.bit_or('012','0123456');
select gms_raw.bit_or('012abc','0123456defg');

select gms_raw.bit_or('abcdefghijklmnopqrst','abcdefghijklmnopqrst');
select gms_raw.bit_or('我爱你中国','我爱你中国');
select gms_raw.bit_or('10', time'01:00:00');
select gms_raw.bit_or('10', '01:00');
select gms_raw.bit_or();
select gms_raw.bit_or('10');
select gms_raw.bit_or('10','10','10');

-- bit_xor
select gms_raw.bit_xor('10','01');
select gms_raw.bit_xor('01','10');
select gms_raw.bit_xor('1010','01');
select gms_raw.bit_xor('01','1010');
select gms_raw.bit_xor('01000111','101011');
select gms_raw.bit_xor(null,'01');
select gms_raw.bit_xor('','01');
select gms_raw.bit_xor('01',null);
select gms_raw.bit_xor('01','');
select gms_raw.bit_xor(null,null);
select gms_raw.bit_xor('','');

select gms_raw.bit_xor('0123456789','0123456789');
select gms_raw.bit_xor('012','0123456');
select gms_raw.bit_xor('012abc','0123456defg');

select gms_raw.bit_xor('abcdefghijklmnopqrst','abcdefghijklmnopqrst');
select gms_raw.bit_xor('我爱你中国','我爱你中国');
select gms_raw.bit_xor('10', time'01:00:00');
select gms_raw.bit_xor('10', '01:00');
select gms_raw.bit_xor();
select gms_raw.bit_xor('10');
select gms_raw.bit_xor('10','10','10');

-- bit_complement
select gms_raw.bit_complement('1010');
select gms_raw.bit_complement('01');
select gms_raw.bit_complement('01000111');
select gms_raw.bit_complement(null);
select gms_raw.bit_complement('');

select gms_raw.bit_complement('0123456789');
select gms_raw.bit_complement('012');
select gms_raw.bit_complement('012abc');

select gms_raw.bit_complement('abcdefghijklmnopqrst');
select gms_raw.bit_complement('我爱你中国');
select gms_raw.bit_complement(time'01:00:00');
select gms_raw.bit_complement('01:00');
select gms_raw.bit_complement();
select gms_raw.bit_complement('10','10');
select gms_raw.bit_complement('10','10', '10');

-- cast_from_binary_double
select * from gms_raw.cast_from_binary_double(3.14);
select * from gms_raw.cast_from_binary_double(3.14, 1);
select * from gms_raw.cast_from_binary_double(3.14, 2);
select * from gms_raw.cast_from_binary_double(3.14, 3);
select * from gms_raw.cast_from_binary_double(3.14, 4);

select * from gms_raw.cast_from_binary_double(0, 3);
select * from gms_raw.cast_from_binary_double(0.0, 3);
select * from gms_raw.cast_from_binary_double(3, 3);
select * from gms_raw.cast_from_binary_double(3.1415844050, 3);
select * from gms_raw.cast_from_binary_double(3.145690, 3);
select * from gms_raw.cast_from_binary_double('3.145690', 3);
select * from gms_raw.cast_from_binary_double(1256456.603, 3);
select * from gms_raw.cast_from_binary_double(3.14e-5, 3);
select * from gms_raw.cast_from_binary_double(3.14e+5, 3);

select * from gms_raw.cast_from_binary_double(NULL, NULL);
select * from gms_raw.cast_from_binary_double(NULL, 1);
select * from gms_raw.cast_from_binary_double('', '');
select * from gms_raw.cast_from_binary_double('', 1);
select * from gms_raw.cast_from_binary_double('abc', 1);
select * from gms_raw.cast_from_binary_double(time '01:00:00', 1);
select * from gms_raw.cast_from_binary_double('01:00', 1);
select * from gms_raw.cast_from_binary_double();
select * from gms_raw.cast_from_binary_double(1, 2, 3);

-- cast_from_binary_float
select * from gms_raw.cast_from_binary_float(3.14);
select * from gms_raw.cast_from_binary_float(3.14, 1);
select * from gms_raw.cast_from_binary_float(3.14, 2);
select * from gms_raw.cast_from_binary_float(3.14, 3);
select * from gms_raw.cast_from_binary_float(3.14, 4);

select * from gms_raw.cast_from_binary_float(0, 3);
select * from gms_raw.cast_from_binary_float(0.0, 3);
select * from gms_raw.cast_from_binary_float(3, 3);
select * from gms_raw.cast_from_binary_float(3.1415844050, 3);
select * from gms_raw.cast_from_binary_float(3.145690, 3);
select * from gms_raw.cast_from_binary_float('3.145690', 3);
select * from gms_raw.cast_from_binary_float(1256456.603, 3);
select * from gms_raw.cast_from_binary_float(3.14e-5, 3);
select * from gms_raw.cast_from_binary_float(3.14e+5, 3);

select * from gms_raw.cast_from_binary_float(NULL, NULL);
select * from gms_raw.cast_from_binary_float(NULL, 1);
select * from gms_raw.cast_from_binary_float('', '');
select * from gms_raw.cast_from_binary_float('', 1);
select * from gms_raw.cast_from_binary_float('abc', 1);
select * from gms_raw.cast_from_binary_float(time '01:00:00', 1);
select * from gms_raw.cast_from_binary_float('01:00', 1);
select * from gms_raw.cast_from_binary_float();
select * from gms_raw.cast_from_binary_float(1, 2, 3);

-- cast_from_binary_integer
-- A库针对两个入参转化为小数会进行截断，openGauss进行四舍五入
-- A库超过bigint的值会转换为int上下界对应的值，openGauss会报错，bigint out of range
select * from gms_raw.cast_from_binary_integer(3);
select * from gms_raw.cast_from_binary_integer(3, 1);
select * from gms_raw.cast_from_binary_integer(3, 2);
select * from gms_raw.cast_from_binary_integer(3, 3);
select * from gms_raw.cast_from_binary_integer(3, 4);

select * from gms_raw.cast_from_binary_integer(35687, 3);
select * from gms_raw.cast_from_binary_integer('35687', 3);
select * from gms_raw.cast_from_binary_integer(33453455, 3);
select * from gms_raw.cast_from_binary_integer(0, 3);
select * from gms_raw.cast_from_binary_integer(0.0, 3);
select * from gms_raw.cast_from_binary_integer(3e10, 3);
select * from gms_raw.cast_from_binary_integer(2.9, 3);
select * from gms_raw.cast_from_binary_integer(3.3, 3);
select * from gms_raw.cast_from_binary_integer(3.5, 3);
select * from gms_raw.cast_from_binary_integer(3.6, 3);
select * from gms_raw.cast_from_binary_integer(3.3, 3.3);
select * from gms_raw.cast_from_binary_integer(3.3, 3.8);
select * from gms_raw.cast_from_binary_integer(33333333333333333333333333333, 3);

-- test boundary value [-2147483648, 2147483647]
select * from gms_raw.cast_from_binary_integer(2147483646, 3);
select * from gms_raw.cast_from_binary_integer(2147483647, 3);
select * from gms_raw.cast_from_binary_integer(2147483648, 3);
select * from gms_raw.cast_from_binary_integer(-2147483646, 3);
select * from gms_raw.cast_from_binary_integer(-2147483647, 3);
select * from gms_raw.cast_from_binary_integer(-2147483648, 3);
select * from gms_raw.cast_from_binary_integer(-2147483649, 3);

select * from gms_raw.cast_from_binary_integer(NULL, NULL);
select * from gms_raw.cast_from_binary_integer(NULL, 1);
select * from gms_raw.cast_from_binary_integer('', '');
select * from gms_raw.cast_from_binary_integer('', 1);
select * from gms_raw.cast_from_binary_integer('abc', 1);
select * from gms_raw.cast_from_binary_integer(time '01:00:00', 1);
select * from gms_raw.cast_from_binary_integer('01:00', 1);
select * from gms_raw.cast_from_binary_integer();
select * from gms_raw.cast_from_binary_integer(1, 2, 3);

-- cast_from_number
-- 针对小数，返回的结果与A库不同
select * from gms_raw.cast_from_number(3.14);
select * from gms_raw.cast_from_number(3.1415926);
select * from gms_raw.cast_from_number(100);
select * from gms_raw.cast_from_number(3e100);
select * from gms_raw.cast_from_number(3e500); -- overflow
select * from gms_raw.cast_from_number(3.1234e500); -- overflow

select * from gms_raw.cast_from_number(NULL);
select * from gms_raw.cast_from_number('');
select * from gms_raw.cast_from_number('abc');

select * from gms_raw.cast_from_number(time '01:00:00');
select * from gms_raw.cast_from_number('01:00');
select * from gms_raw.cast_from_number();
select * from gms_raw.cast_from_number(1, 2);
select * from gms_raw.cast_from_number(1, 2, 3);

-- cast_to_binary_double
select gms_raw.cast_to_binary_double(gms_raw.cast_from_binary_double(3.14, 1), 1);
select * from gms_raw.cast_from_binary_double(3.14, 1);
select gms_raw.cast_to_binary_double('40091EB851EB851F'); --3.14
select gms_raw.cast_to_binary_double('40091EB851EB851F', 1);
select gms_raw.cast_to_binary_double('1F85EB51B81E0940', 2);
select gms_raw.cast_to_binary_double('1F85EB51B81E0940', 3);
select gms_raw.cast_to_binary_double(gms_raw.cast_from_binary_double(3.14, 1), 3); -- other value
select gms_raw.cast_to_binary_double(gms_raw.cast_from_binary_double(7.9824696849641e-157, 1), 3); -- 3.14

select * from gms_raw.cast_from_binary_double(3.1415844050, 3);
select gms_raw.cast_to_binary_double('F9C92801F7210940', 3);
select gms_raw.cast_to_binary_double(gms_raw.cast_from_binary_double(3.1415844050, 1), 1);
select * from gms_raw.cast_from_binary_double(3.145690, 3);
select gms_raw.cast_to_binary_double('7CD5CA845F2A0940', 3);
select gms_raw.cast_to_binary_double(gms_raw.cast_from_binary_double(3.145690, 1), 1);
select * from gms_raw.cast_from_binary_double('3.145690', 3);
select gms_raw.cast_to_binary_double('7CD5CA845F2A0940', 3);
select gms_raw.cast_to_binary_double(gms_raw.cast_from_binary_double('3.145690', 1), 1);
select * from gms_raw.cast_from_binary_double(1256456.603, 3);
select gms_raw.cast_to_binary_double('3F355E9A082C3341', 3);
select gms_raw.cast_to_binary_double(gms_raw.cast_from_binary_double(1256456.603, 1), 1);
select * from gms_raw.cast_from_binary_double(3.14e-5, 3);
select gms_raw.cast_to_binary_double('7FB7E5C86F76003F', 3);
select gms_raw.cast_to_binary_double(gms_raw.cast_from_binary_double(3.14e-5, 1), 1);
select * from gms_raw.cast_from_binary_double(3.14e+5, 3);
select gms_raw.cast_to_binary_double('00000000402A1341', 3);
select gms_raw.cast_to_binary_double(gms_raw.cast_from_binary_double(3.14e+5, 1), 1);

select gms_raw.cast_to_binary_double('402A134111112222', 3);
select gms_raw.cast_to_binary_double('402A13411111222211',3); -- exceeds 8 bytes, will be truncated
select gms_raw.cast_to_binary_double('402A1341111122221122', 3); -- exceeds 8 bytes, will be truncated
select gms_raw.cast_to_binary_double('402A134111112222112233', 3); -- exceeds 8 bytes, will be truncated

select gms_raw.cast_to_binary_double('12345678', 3); -- error, when not enough 8 bytes
select gms_raw.cast_to_binary_double('34567812345678', 3); -- error, when not enough 8 bytes
select gms_raw.cast_to_binary_double('134567812345678', 3); -- 15 hex digits, will add 0 in the front
select gms_raw.cast_to_binary_double('0134567812345678', 3); -- same result as the above result

select * from gms_raw.cast_to_binary_double(NULL, NULL);
select * from gms_raw.cast_to_binary_double(NULL, 1);
select * from gms_raw.cast_to_binary_double('', '');
select * from gms_raw.cast_to_binary_double('', 1);
select * from gms_raw.cast_to_binary_double('abc', 1);
select * from gms_raw.cast_to_binary_double(time '01:00:00', 1);
select * from gms_raw.cast_to_binary_double('01:00', 1);
select * from gms_raw.cast_to_binary_double();
select * from gms_raw.cast_to_binary_double(1, 2, 3);

-- cast_to_binary_float
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.14));
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.14, 1), 1);
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.14, 2), 2);
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.14, 3), 4);
select * from gms_raw.cast_from_binary_float(3.14, 1);
select gms_raw.cast_to_binary_float('4048F5C3'); -- 3.14
select gms_raw.cast_to_binary_float('4048F5C3', 1);
select gms_raw.cast_to_binary_float('C3F54840', 2);
select gms_raw.cast_to_binary_float('C3F54840', 3);
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.14, 1), 3); -- other value
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(-490.564, 1), 3); -- not 3.14, there may be a loss of accuracy

select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.14, 1), 2); -- not 3.14
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.14, 1), 3); -- not 3.14
select gms_raw.cast_from_binary_float(3.14, 1);
select gms_raw.cast_to_binary_float('4048F5C3',3);

select * from gms_raw.cast_from_binary_float(3.1415844050, 3);
select gms_raw.cast_to_binary_float('B80F4940', 3);
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.1415844050, 1), 1);
select * from gms_raw.cast_from_binary_float(3.145690, 3);
select gms_raw.cast_to_binary_float('FC524940', 3);
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.145690, 1), 1);
select * from gms_raw.cast_from_binary_float('3.145690', 3);
select gms_raw.cast_to_binary_float('FC524940', 3);
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float('3.145690', 1), 1);
select * from gms_raw.cast_from_binary_float(1256456.603, 3);
select gms_raw.cast_to_binary_float('45609949', 3);
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(1256456.603, 1), 1);
select * from gms_raw.cast_from_binary_float(3.14e-5, 3);
select gms_raw.cast_to_binary_float('7EB30338', 3);
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.14e-5, 1), 1);
select * from gms_raw.cast_from_binary_float(3.14e+5, 3);
select gms_raw.cast_to_binary_float('00529948', 3);
select gms_raw.cast_to_binary_float(gms_raw.cast_from_binary_float(3.14e+5, 1), 1);

select gms_raw.cast_to_binary_float('4048F5C3', 3);
select gms_raw.cast_to_binary_float('4048F5C31122',3); -- exceeds 4 bytes, will be truncated
select gms_raw.cast_to_binary_float('4048F5C3112230', 3); -- exceeds 4 bytes, will be truncated
select gms_raw.cast_to_binary_float('04048F5C311223', 3); -- exceeds 4 bytes, will be truncated
select gms_raw.cast_to_binary_float('4048F5C311223', 3); -- odd numbers will be padded with 0 in the front

select gms_raw.cast_to_binary_float('1234', 3); -- error, when not enough 4 bytes
select gms_raw.cast_to_binary_float('345678', 3); -- error, when not enough 4 bytes
select gms_raw.cast_to_binary_float('1234567', 3); -- 7 hex digits, will add 0 in the front
select gms_raw.cast_to_binary_float('01234567', 3); --same result as the above result

select * from gms_raw.cast_to_binary_float(NULL, NULL);
select * from gms_raw.cast_to_binary_float(NULL, 1);
select * from gms_raw.cast_to_binary_float('', '');
select * from gms_raw.cast_to_binary_float('', 1);
select * from gms_raw.cast_to_binary_float('abc', 1);
select * from gms_raw.cast_to_binary_float(time '01:00:00', 1);
select * from gms_raw.cast_to_binary_float('01:00', 1);
select * from gms_raw.cast_to_binary_float();
select * from gms_raw.cast_to_binary_float(1, 2, 3);

-- cast_to_binary_integer
select gms_raw.cast_from_binary_integer(3);
select gms_raw.cast_to_binary_integer('00000003');
select gms_raw.cast_to_binary_integer('00000003', 1);
select gms_raw.cast_to_binary_integer('03000000', 2);
select gms_raw.cast_to_binary_integer('03000000', 3);
select gms_raw.cast_to_binary_integer('03000000', 4);

select gms_raw.cast_from_binary_integer(35687, 3);
select gms_raw.cast_to_binary_integer('678B0000', 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(35687, 3), 3);
select gms_raw.cast_from_binary_integer('35687', 3);
select gms_raw.cast_to_binary_integer('678B0000', 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer('35687', 3), 3);
select gms_raw.cast_from_binary_integer(33453455, 3);
select gms_raw.cast_to_binary_integer('8F75FE01', 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(33453455, 3), 3);
select gms_raw.cast_from_binary_integer(0, 3);
select gms_raw.cast_to_binary_integer('00000000', 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(0, 3), 3);
select gms_raw.cast_from_binary_integer(0.0, 3);
select gms_raw.cast_to_binary_integer('00000000', 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(0.0, 3), 3);
select gms_raw.cast_from_binary_integer(3e10, 3);
select gms_raw.cast_to_binary_integer('FFFFFF7F', 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(3e10, 3), 3); -- 2147483647

select gms_raw.cast_from_binary_integer(2.9, 3); -- 四舍五入，结果为3
select gms_raw.cast_to_binary_integer('03000000', 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(2.9, 3), 3);

select gms_raw.cast_from_binary_integer(3.3, 3);
select gms_raw.cast_to_binary_integer('03000000', 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(3.3, 3), 3);

select gms_raw.cast_from_binary_integer(3.5, 3); -- 四舍五入，结果为4
select gms_raw.cast_to_binary_integer('04000000', 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(3.5, 3), 3);

select gms_raw.cast_from_binary_integer(3.6, 3);
select gms_raw.cast_to_binary_integer('04000000', 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(3.6, 3), 3);

select gms_raw.cast_from_binary_integer(3.3, 3.3);
select gms_raw.cast_to_binary_integer('03000000', 3.3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(3.3, 3.3), 3.3);

select gms_raw.cast_from_binary_integer(3.3, 3.8); --四舍五入，第二个参数为4，报错
select gms_raw.cast_to_binary_integer('03000000', 3.8);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(3.3, 3.8), 3.8);

-- test boundary value [-2147483648, 2147483647]
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(2147483646, 3), 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(2147483647, 3), 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(2147483648, 3), 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(-2147483646, 3), 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(-2147483647, 3), 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(-2147483648, 3), 3);
select gms_raw.cast_to_binary_integer(gms_raw.cast_from_binary_integer(-2147483649, 3), 3);

select gms_raw.cast_to_binary_integer(NULL, NULL);
select gms_raw.cast_to_binary_integer(NULL, 1);
select gms_raw.cast_to_binary_integer('', '');
select gms_raw.cast_to_binary_integer('', 1);
select gms_raw.cast_to_binary_integer('zzzzzzzz', 1);
select gms_raw.cast_to_binary_integer(time '01:00:00', 1);
select gms_raw.cast_to_binary_integer('01:00', 1);
select gms_raw.cast_to_binary_integer();
select gms_raw.cast_to_binary_integer(1, 2, 3);

-- cast_to_number
select * from gms_raw.cast_from_number(3.14);
select * from gms_raw.cast_to_number('008103007805');
select * from gms_raw.cast_to_number('08103007805'); -- when odd number, add 0 in the front
select * from gms_raw.cast_to_number(gms_raw.cast_from_number(3.14));
select * from gms_raw.cast_from_number(3.1415926);
select * from gms_raw.cast_to_number('8083030087052C24');
select * from gms_raw.cast_to_number(gms_raw.cast_from_number(3.1415926));
select * from gms_raw.cast_to_number('8083030087052C241111');
select * from gms_raw.cast_to_number('8083030087052C2411');
select * from gms_raw.cast_from_number(100);
select * from gms_raw.cast_to_number('00806400');
select * from gms_raw.cast_to_number(gms_raw.cast_from_number(100));
select * from gms_raw.cast_from_number(3e100);
select * from gms_raw.cast_to_number('19800300');
select * from gms_raw.cast_to_number(gms_raw.cast_from_number(3e100));
select * from gms_raw.cast_from_number(3e500); -- overflow
select * from gms_raw.cast_to_number(gms_raw.cast_from_number(3e500));
select * from gms_raw.cast_from_number(3.1234e500); -- overflow
select * from gms_raw.cast_to_number(gms_raw.cast_from_number(3.1234e500));

select * from gms_raw.cast_to_number(NULL);
select * from gms_raw.cast_to_number('');

select * from gms_raw.cast_to_number(time '01:00:00');
select * from gms_raw.cast_to_number('01:00');
select * from gms_raw.cast_to_number('你好');
select * from gms_raw.cast_to_number();
select * from gms_raw.cast_to_number(1, 2);
select * from gms_raw.cast_to_number(1, 2, 3);

-- cast_to_nvarchar2
select * from gms_raw.cast_to_nvarchar2('12345678');
select * from gms_raw.cast_to_nvarchar2('0000');
select * from gms_raw.cast_to_nvarchar2('0012');
select * from gms_raw.cast_to_nvarchar2('001234');
select * from gms_raw.cast_to_nvarchar2('1234');
select * from gms_raw.cast_to_nvarchar2('655655');

select * from gms_raw.cast_to_nvarchar2('');
select * from gms_raw.cast_to_nvarchar2(NULL);

select * from gms_raw.cast_to_nvarchar2('\x124Vx');
select * from gms_raw.cast_to_nvarchar2('x124Vx');
select * from gms_raw.cast_to_nvarchar2(time '01:00:00');
select * from gms_raw.cast_to_nvarchar2('01:00');
select * from gms_raw.cast_to_nvarchar2('你好');
select * from gms_raw.cast_to_nvarchar2();
select * from gms_raw.cast_to_nvarchar2(1, 2);
select * from gms_raw.cast_to_nvarchar2(1, 2, 3);

-- cat_to_raw
select * from gms_raw.cast_to_raw('12345');
select * from gms_raw.cast_to_raw('abcdefghijklmn');
select * from gms_raw.cast_to_raw('你好明天');
select * from gms_raw.cast_to_raw('你好openGauss');
select * from gms_raw.cast_to_raw('你好：（&%￥$)');

select * from gms_raw.cast_to_raw(NULL);
select * from gms_raw.cast_to_raw('');
select * from gms_raw.cast_to_raw(' ');
select * from gms_raw.cast_to_raw('    ');

select * from gms_raw.cast_to_raw();
select * from gms_raw.cast_to_raw(1, 2);
select * from gms_raw.cast_to_raw(1, 2, 3);

-- cast_to_varchar2
select * from gms_raw.cast_to_raw('12345');
select * from gms_raw.cast_to_varchar2('3132333435');
select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('12345'));

select * from gms_raw.cast_to_raw('abcdefghijklmn');
select * from gms_raw.cast_to_varchar2('6162636465666768696A6B6C6D6E');
select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('abcdefghijklmn'));

select * from gms_raw.cast_to_raw('你好明天');
select * from gms_raw.cast_to_varchar2('E4BDA0E5A5BDE6988EE5A4A9');
select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('你好明天'));

select * from gms_raw.cast_to_raw('你好openGauss');
select * from gms_raw.cast_to_varchar2('E4BDA0E5A5BD6F70656E4761757373');
select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('你好openGauss'));

select * from gms_raw.cast_to_raw('你好：（&%￥$)');
select * from gms_raw.cast_to_varchar2('E4BDA0E5A5BDEFBC9AEFBC882625EFBFA52429');
select gms_raw.cast_to_varchar2(gms_raw.cast_to_raw('你好：（&%￥$)'));

select * from gms_raw.cast_to_varchar2(NULL);
select * from gms_raw.cast_to_varchar2('');

select * from gms_raw.cast_to_varchar2('zzzmmmnnn');
select * from gms_raw.cast_to_varchar2('你好openGauss');
select * from gms_raw.cast_to_varchar2(time '01:00:00');
select * from gms_raw.cast_to_varchar2(time '01:00:00', 1);
select * from gms_raw.cast_to_varchar2('01:00');
select * from gms_raw.cast_to_varchar2('01:00', 1);

select * from gms_raw.cast_to_varchar2();
select * from gms_raw.cast_to_varchar2(1, 2);
select * from gms_raw.cast_to_varchar2(1, 2, 3);

-- compare
select gms_raw.compare(NULL, NULL, NULL);
select gms_raw.compare(NULL, '01');
select gms_raw.compare(NULL, '01', NULL);
select gms_raw.compare('01', NULL);
select gms_raw.compare('01', NULL, NULL);

select gms_raw.compare('', '', '');
select gms_raw.compare('', '01');
select gms_raw.compare('', '01', NULL);
select gms_raw.compare('01', '');
select gms_raw.compare('01', '', '');

select gms_raw.compare('01', '', '0');
select gms_raw.compare('01', '', '01');
select gms_raw.compare('01', '', '012');
select gms_raw.compare('01', '', '0123');

select gms_raw.compare('01', '0123', '0');
select gms_raw.compare('01', '0100', '01');
select gms_raw.compare('01', '0111', '012');
select gms_raw.compare('01', '0110', '0123');
select gms_raw.compare('01', '0111', '11');

select gms_raw.compare('1', '0101', '1');
select gms_raw.compare('1', '101', '1');
select gms_raw.compare('1', '101', '10');

select gms_raw.compare('01', '0123', '1');
select gms_raw.compare('01', '0100', '11');
select gms_raw.compare('01', '0111', '112');
select gms_raw.compare('01', '0110', '1123');

select gms_raw.compare('112233445566', '1122334455', '66');
select gms_raw.compare('112233445566', '1122', '12');
select gms_raw.compare('112233445566', '11223344556677', '77');

select gms_raw.compare('01zzzz', '0123', '01');
select gms_raw.compare('01', '0123zzz', '01');
select gms_raw.compare('01', '0123', '0123zzz');

select gms_raw.compare();
select gms_raw.compare('12');
select gms_raw.compare('12', '12', '12', '233');

select gms_raw.compare('12', 'time 12:00:00', '12');
select gms_raw.compare('time 12:00:00', '12', '12');
select gms_raw.compare('12', '12', 'time 12:00:00');

-- concat
select gms_raw.concat();
select gms_raw.concat('11'); 
select gms_raw.concat('00', '11');
select gms_raw.concat('0', '1');
select gms_raw.concat('01', '01');
select gms_raw.concat('11', '22', '33');
select gms_raw.concat('11', '22', '33', '44');
select gms_raw.concat('11', '22', '33', '44', '55');
select gms_raw.concat('11', '22', '33', '44', '55', '66');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', '99');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', '99', '00');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', '99', '00', 'aa');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', '99', '00', 'aa', 'bb');
select gms_raw.concat(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
select gms_raw.concat('', '', '', '', '', '', '', '', '', '', '', '');
select gms_raw.concat('01', '0110', '1123', '01', '0110', '1123', '01', '0110', '1123', '01', '0110', '1123');
select gms_raw.concat('11', '22', '33', NULL, NULL);
select gms_raw.concat('11', '22', '33', NULL, NULL, NULL, NULL);
select gms_raw.concat('11', '22', '33', NULL, NULL, NULL, NULL, '', '', '');

select gms_raw.concat('zz', '22', '33', '44', '55', '66', '77', '88', '99', '00', 'aa', 'bb');
select gms_raw.concat('11', 'zz', '33', '44', '55', '66', '77', '88', '99', '00', 'aa', 'bb');
select gms_raw.concat('11', '22', 'zz', '44', '55', '66', '77', '88', '99', '00', 'aa', 'bb');
select gms_raw.concat('11', '22', '33', 'zz', '55', '66', '77', '88', '99', '00', 'aa', 'bb');
select gms_raw.concat('11', '22', '33', '44', 'zz', '66', '77', '88', '99', '00', 'aa', 'bb');
select gms_raw.concat('11', '22', '33', '44', '55', 'zz', '77', '88', '99', '00', 'aa', 'bb');
select gms_raw.concat('11', '22', '33', '44', '55', '66', 'zz', '88', '99', '00', 'aa', 'bb');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', 'zz', '99', '00', 'aa', 'bb');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', 'zz', '00', 'aa', 'bb');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', '99', 'zz', 'aa', 'bb');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', '99', '00', 'zz', 'bb');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', '99', '00', 'aa', 'zz');

select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', '99', '00', 'aa', 'bb', 'cc');
select gms_raw.concat(time '01:00:00', '22', '33', '44', '55', '66', '77', '88', '99', '00', 'aa', 'bb');
select gms_raw.concat('11', '22', '33', '44', time '01:00:00', '66', '77', '88', '99', '00', 'aa', 'bb');
select gms_raw.concat('11', '22', '33', '44', '55', '66', '77', '88', '99', '00', 'aa', time '01:00:00');

--convert
select gms_raw.convert('31', 'utf8', 'utf8');
select gms_raw.convert('31', 'gbk', 'utf8'); --openGauss gbk等价于A库ZHS16GBK
select gms_raw.convert('31', 'gbk', 'gbk');
select gms_raw.convert('31', 'utf8', 'gbk');

select * from gms_raw.cast_to_raw('你好明天');
select * from gms_raw.convert('E4BDA0E5A5BDE6988EE5A4A9', 'gbk', 'gbk');
select * from gms_raw.convert('E4BDA0E5A5BDE6988EE5A4A9', 'utf8', 'utf8');
select * from gms_raw.convert('E4BDA0E5A5BDE6988EE5A4A9', 'utf8', 'gbk');
select * from gms_raw.convert('E6B5A3E78AB2E382BDE98F84E5BAA1E38189', 'gbk', 'utf8');
select * from gms_raw.convert('E4BDA0E5A5BDE6988EE5A4A9', 'gbk', 'utf8');
select * from gms_raw.convert('C4E3BAC3C3F7CCEC', 'utf8', 'gbk');
select * from gms_raw.cast_to_varchar2('E4BDA0E5A5BDE6988EE5A4A9');

select gms_raw.convert(time '01:00:00', 'utf8', 'gbk');
select gms_raw.convert('01:00', 'utf8', 'gbk');
select gms_raw.convert('11', 'charset', 'utf8');
select gms_raw.convert('11', 'utf8', 'charset');

select gms_raw.convert(NULL, NULL, NULL);
select gms_raw.convert('', '', '');
select gms_raw.convert('11', NULL, NULL);
select gms_raw.convert(NULL, 'utf8', 'gbk');

select gms_raw.convert();
select gms_raw.convert('11');
select gms_raw.convert('11', 'to_charset');
select gms_raw.convert('11', 'to_charset', 'from_charset', 'param4');

-- copies
select gms_raw.copies('001122', 1);
select gms_raw.copies('001122', 3);
select gms_raw.copies('00112233', 3);
select gms_raw.copies('0011223344', 4);
select gms_raw.copies('123', 3);
select gms_raw.copies('00112233', 3.2); -- 小数四舍五入，和A库一致
select gms_raw.copies('00112233', 3.6);

select gms_raw.copies('', 2);
select gms_raw.copies(NULL, 2);
select gms_raw.copies('1234', NULL);
select gms_raw.copies('1234', '');

select gms_raw.copies('00112233', 0);
select gms_raw.copies('00112233', -1);

select gms_raw.copies();
select gms_raw.copies('0011223344');
select gms_raw.copies('0011223344', 1, 2);

select gms_raw.copies('0011zzmmnn', 4);
select gms_raw.copies('01:00:00', 4);
select gms_raw.copies(time '01:00:00', 4);

select gms_raw.copies('12', 1073733623); -- param2 more than max value
select gms_raw.copies('1122', 1073733615); -- total length more than max value

-- reverse
select gms_raw.reverse('1');
select gms_raw.reverse('01');
select gms_raw.reverse('1122');
select gms_raw.reverse('11223344');
select gms_raw.reverse('12345678');

select gms_raw.reverse('');
select gms_raw.reverse(NULL);
select gms_raw.reverse(time '01:00:00');
select gms_raw.reverse('01:00:00');
select gms_raw.reverse('zzzz');

select gms_raw.reverse();
select gms_raw.reverse('11', 2);
select gms_raw.reverse('11', 2, 3);

-- translate
select gms_raw.translate('1100110011', '11', '12');
select gms_raw.translate('1100110011', '1100', '12');
select gms_raw.translate('1110011100111', '111', '12');
select gms_raw.translate('1110011100111', '111', '1234');
select gms_raw.translate('01110011100111','0111','123456');
select gms_raw.translate('01110011100111','011111','123434');
select gms_raw.translate('01110011100111','011111','123456'); -- 以第一次出现为准，11对应34，不是56
select gms_raw.translate('aabbccddeeffaabbccddeeff','aaaabbbbcccc', 'bbccaaffeeaa');

select gms_raw.translate('1011', '00ff', '00');
select gms_raw.translate('1011', '1011', '1011');
select gms_raw.translate('1011', '1011', '3344');
select gms_raw.translate('aabbccdd001122','aabbccdd','eeff'); -- ccdd将被删除
select gms_raw.translate('aabbccdd001122','aabbccdd','eeff001133'); -- 33没有对应的
select gms_raw.translate('aabbccdd001122','aaaabbcc','eeff0011'); -- aa对应ee

select gms_raw.translate('1122', '1122', '3344');
select gms_raw.translate('', '1122', '3344');
select gms_raw.translate(NULL, '1122', '3344');
select gms_raw.translate('1122', '', '3344');
select gms_raw.translate('1122', NULL, '3344');
select gms_raw.translate('1122', '1122', '');
select gms_raw.translate('1122', '1122', NULL);

select gms_raw.translate(time '01:00:00', '1122', '3344');
select gms_raw.translate('1122', time '01:00:00', '3344');
select gms_raw.translate('1122', '1122', time '01:00:00');

select gms_raw.translate('aabbccddmmnngg','aabbccdd','eeff');
select gms_raw.translate('aabbccdd','aabbccddmm','eeffgghhii');
select gms_raw.translate('aabbccdd','aaaabbcc','eeffmm');

select gms_raw.translate();
select gms_raw.translate('1122');
select gms_raw.translate('1122', '3344');
select gms_raw.translate('1122', '3344', '5566', '7788');

-- transliterate
select gms_raw.transliterate('aabbccddeeffaabbccddeeff'); -- If from is null, replace all with pad
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', NULL);
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', '');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', NULL, NULL);
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', '', '');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', NULL, NULL, NULL);
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', '', '', '');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', NULL, NULL, 'aa');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', '', '', 'aa');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', NULL, 'bb', NULL);
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', '', 'bb', '');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', NULL, 'bb', 'cc');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', '', 'bb', 'cc');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', NULL, 'bb', 'aa');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', '', 'bb', 'aa');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', 'bb', NULL, 'aa');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', 'bb', '', 'aa');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', '11', 'bb', NULL);
select gms_raw.transliterate('aabbccddeeffaabbccddeeff', '11', 'bb', '');

select gms_raw.transliterate('aabbccddee','bb', 'cc', 'aa');
select gms_raw.transliterate('aabbccddee','bb', 'ccee', 'aa');
select gms_raw.transliterate('aabbccddee','bb', 'ccee', 'aabb');
select gms_raw.transliterate('aabbccddee','bbddff', 'ccee', 'aa');
select gms_raw.transliterate('aabbccddee','bbddff', 'ccee', 'aabb');
select gms_raw.transliterate('aabbccddeeff','bbddff', 'ccee', 'aabb');

select gms_raw.transliterate('aabbccddeeff','bbddff11', 'cceeccee', 'aabb');
select gms_raw.transliterate('aabbccddeeffaabbccddeeff','bbddff11', 'cceeccee', 'aabb');

select gms_raw.transliterate('1122');
select gms_raw.transliterate('1122', '3344');
select gms_raw.transliterate('1122', '3344', '5566');
select gms_raw.transliterate('1122', '3344', '5566', '7788');

select gms_raw.transliterate(time '01:00:00', '1122', '3344', '5566');
select gms_raw.transliterate('1122', time '01:00:00', '3344', '5566');
select gms_raw.transliterate('1122', '1122', time '01:00:00', '5566');
select gms_raw.transliterate('1122', '1122', '3344', time '01:00:00');

select gms_raw.transliterate('aabbccddmmnngg','aabbccdd','eeff', '1122');
select gms_raw.transliterate('aabbccdd','aabbccddmm','eeffgghhii', '1122');
select gms_raw.transliterate('aabbccdd','aaaabbcc','eeffmm', '1122');
select gms_raw.transliterate('aabbccdd','aaaabbcc','eeff', '1122mm');

select gms_raw.transliterate();
select gms_raw.transliterate(NULL);
select gms_raw.transliterate('');

-- xrange
select gms_raw.xrange(); -- A库显示超过255个字符会截断，openGauss不会截断，全部显示
select gms_raw.xrange(NULL, NULL);
select gms_raw.xrange('', '');
select gms_raw.xrange(NULL, '08');
select gms_raw.xrange('', '08');
select gms_raw.xrange('1F', NULL);
select gms_raw.xrange('1F', '');
select gms_raw.xrange('10');

select gms_raw.xrange('00', 'ff');
select gms_raw.xrange('0012', 'ff33');
select gms_raw.xrange('00', '33');
select gms_raw.xrange('10', '10');
select gms_raw.xrange('1', 'f');
select gms_raw.xrange('01', '0f');
select gms_raw.xrange('33', '33');
select gms_raw.xrange('33', '44');
select gms_raw.xrange('44', '33');
select gms_raw.xrange('10', '14');
select gms_raw.xrange('14', '10');
select gms_raw.xrange('4411', '3311');
select gms_raw.xrange('441122', '331122');

select gms_raw.xrange('1', '2', '3');
select gms_raw.xrange(time '01:00:00', '11');
select gms_raw.xrange('11', time '01:00:00');
select gms_raw.xrange('zzz', '11');
select gms_raw.xrange('11', 'zzz');

SELECT GMS_RAW.BIT_AND(GMS_raw.cast_to_raw('1010'), GMS_raw.cast_to_raw('1111')) AS result;
SELECT GMS_RAW.BIT_COMPLEMENT(GMS_raw.cast_to_raw('1010')) AS result;
SELECT GMS_RAW.BIT_OR(GMS_raw.cast_to_raw('1010'), GMS_raw.cast_to_raw('1111')) AS result;
SELECT GMS_RAW.BIT_XOR(GMS_raw.cast_to_raw('1010'), GMS_raw.cast_to_raw('1111')) AS result;

SELECT GMS_RAW.CAST_FROM_BINARY_DOUBLE(1.1) AS result;
SELECT GMS_RAW.CAST_FROM_BINARY_FLOAT(1.22) AS result;
SELECT GMS_RAW.CAST_FROM_BINARY_INTEGER(3) AS result;
SELECT GMS_RAW.CAST_FROM_NUMBER(33.34) AS result;
SELECT GMS_RAW.CAST_TO_RAW('Hello') AS raw_value;
SELECT GMS_raw.cast_to_binary_float('42F6E666');
SELECT GMS_raw.cast_to_binary_float('42F6E666');
SELECT GMS_raw.cast_to_binary_integer('00000064');
SELECT GMS_raw.cast_to_number('C202');
SELECT GMS_raw.cast_to_varchar2('213421342134213421342134');
SELECT GMS_raw.cast_to_nvarchar2('213421342134213421342134');

SELECT GMS_RAW.COMPARE(GMS_raw.cast_to_raw('Hello'), GMS_raw.cast_to_raw('Hello')) AS comparison_result;
SELECT GMS_RAW.CONCAT(GMS_raw.cast_to_raw('Hello'), GMS_raw.cast_to_raw('World')) AS concatenated_value;
SELECT GMS_RAW.CONVERT(GMS_raw.cast_to_raw('Hello'), 'UTF8', 'US7ASCII') AS converted_value; -- ERROR, US7ASCII编码集og没有, 换成GBK结果一致
SELECT GMS_RAW.CONVERT(GMS_raw.cast_to_raw('Hello'), 'UTF8', 'GBK') AS converted_value;
SELECT GMS_RAW.COPIES(GMS_raw.cast_to_raw('Hello'), 3) AS repeated_value;
SELECT GMS_RAW.REVERSE(GMS_raw.cast_to_raw('Hello')) AS reversed_value;
SELECT GMS_RAW.XRANGE(GMS_raw.cast_to_raw('A'), GMS_raw.cast_to_raw('Z')) AS range_value;

CREATE OR REPLACE FUNCTION trans_demo(pin IN VARCHAR2)
RETURN VARCHAR2 IS
 r_in  RAW(2000);
 r_out RAW(2000);
 r_ul  RAW(64);
 r_lu  RAW(64);
BEGIN
  r_in := GMS_raw.cast_to_raw(pin);
  r_ul := GMS_raw.cast_to_raw('ABCDEFabcdef');
  r_lu := GMS_raw.cast_to_raw('abcdefABCDEF');

  r_out := GMS_raw.translate(r_in , r_ul, r_lu);

  return(GMS_raw.cast_to_varchar2(r_out));
END ;
/
select trans_demo('ABCDEFGabcdefg');

CREATE OR REPLACE FUNCTION tl_demo(pin IN VARCHAR2)
RETURN VARCHAR2 AUTHID DEFINER IS
 r_in  RAW(2000);
 r_out RAW(2000);
 r_up  RAW(64);
 r_lo  RAW(64);
 r_un  RAW(64) := GMS_raw.cast_to_raw('_');
BEGIN
  r_in := GMS_raw.cast_to_raw(pin);
  r_up := GMS_raw.cast_to_raw('ABCDEF. ');
  r_lo := GMS_raw.cast_to_raw('abcdef');

  r_out := GMS_raw.transliterate(r_in , r_lo, r_up, r_un);

  return(GMS_raw.cast_to_varchar2(r_out));
END ;
/
select tl_demo('ABCDEFG. abcdefg');

drop schema gms_raw;
drop schema gms_raw cascade;

drop extension gms_raw;

-- test create again
create extension gms_raw;
drop extension gms_raw;
