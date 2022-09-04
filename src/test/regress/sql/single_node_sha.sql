-- A format database not support <=>
select sha(null);
select sha('');
select sha('test sha AAA 123 + 中文测试');

select sha1(null);
select sha1('');
select sha1('test sha AAA 123 + 中文测试');

select sha2(null,224);
select sha2('',224);
select sha2('test sha AAA 123 + 中文测试',224);

select sha2(null,256);
select sha2('',256);
select sha2('test sha AAA 123 + 中文测试',256);

select sha2(null,384);
select sha2('',384);
select sha2('test sha AAA 123 + 中文测试',384);

select sha2(null,512);
select sha2('',512);
select sha2('test sha AAA 123 + 中文测试',512);

select sha2(null,160);
select sha2('test',160);
select sha2('test sha AAA 123 + 中文测试',160);

DROP DATABASE IF EXISTS test_sha;
CREATE DATABASE test_sha dbcompatibility = 'B';
\c test_sha
 
-- test sha
select sha(null);
select sha('');
select sha('      ');
select sha('abcfsasfasgagasgagas');
select sha('ABCFAFASFASFAFASSFSASFSAFASFA');
select sha('4123421341265262547472562562');
select sha('test sha ABC 123');
select sha('中文测试');
select sha('-=!@#$%^&*";./?<>|:[]{}+\,*.');
select sha('test sha AAA 123 + 中文测试');

--test sha fail example
select sha();
select sha(abc);
select sha1('abc',0);

-- test sha1
select sha1(null);
select sha1('');
select sha1('      ');
select sha1('abcfsasfasgagasgagas');
select sha1('ABCFAFASFASFAFASSFSASFSAFASFA');
select sha1('4123421341265262547472562562');
select sha1('test sha ABC 123');
select sha1('中文测试');
select sha1('-=!@#$%^&*";./?<>|:[]{}+\,*.');
select sha1('test sha AAA 123 + 中文测试');

--test sha1 fail example
select sha1();
select sha1(abc);
select sha1('abc',1);
 
-- test sha224
select sha2(null,224);
select sha2('',224);
select sha2('      ',224);
select sha2('abcfsasfasgagasgagas',224);
select sha2('ABCFAFASFASFAFASSFSASFSAFASFA',224);
select sha2('4123421341265262547472562562',224);
select sha2('test sha ABC 123',224);
select sha2('中文测试',224);
select sha2('-=!@#$%^&*";./?<>|:[]{}+\,*.',224);
select sha2('test sha AAA 123 + 中文测试',224);

-- test sha256
select sha2(null,256);
select sha2('',256);
select sha2('      ',256);
select sha2('abcfsasfasgagasgagas',256);
select sha2('ABCFAFASFASFAFASSFSASFSAFASFA',256);
select sha2('4123421341265262547472562562',256);
select sha2('test sha ABC 123',256);
select sha2('中文测试',256);
select sha2('-=!@#$%^&*";./?<>|:[]{}+\,*.',256);
select sha2('test sha AAA 123 + 中文测试',256);

-- test sha0
select sha2(null,0);
select sha2('',0);
select sha2('      ',0);
select sha2('abcfsasfasgagasgagas',0);
select sha2('ABCFAFASFASFAFASSFSASFSAFASFA',0);
select sha2('4123421341265262547472562562',0);
select sha2('test sha ABC 123',0);
select sha2('中文测试',0);
select sha2('-=!@#$%^&*";./?<>|:[]{}+\,*.',0);
select sha2('test sha AAA 123 + 中文测试',0);

-- test sha384
select sha2(null,384);
select sha2('',384);
select sha2('      ',384);
select sha2('abcfsasfasgagasgagas',384);
select sha2('ABCFAFASFASFAFASSFSASFSAFASFA',384);
select sha2('4123421341265262547472562562',384);
select sha2('test sha ABC 123',384);
select sha2('中文测试',384);
select sha2('-=!@#$%^&*";./?<>|:[]{}+\,*.',384);
select sha2('test sha AAA 123 + 中文测试',384);

-- test sha512
select sha2(null,512);
select sha2('',512);
select sha2('      ',512);
select sha2('abcfsasfasgagasgagas',512);
select sha2('ABCFAFASFASFAFASSFSASFSAFASFA',512);
select sha2('4123421341265262547472562562',512);
select sha2('test sha ABC 123',512);
select sha2('中文测试',512);
select sha2('-=!@#$%^&*";./?<>|:[]{}+\,*.',512);
select sha2('test sha AAA 123 + 中文测试',512);

-- test null input param
select sha2('aasdfghj',null);
select sha2(null,null);

-- test input param is 160
select sha2(null,160);
select sha2('test',160);
select sha2('test sha AAA 123 + 中文测试',160);
 
-- test test sha2 fail example
select sha2();
select sha2(abc);
select sha2('abc');
select sha2('abc',128);
select sha2('abc',-1);

--test sha/sha2 as column
create table test(a int, b varchar(10));
insert into test values(1111,'adaAFG');
insert into test values(2222,'abcFGSF');
insert into test values(3333,'dsadaWE');
insert into test values(4444,'sdafQW');

select sha(a), sha(b) from test;
select sha1(a), sha1(b) from test;
select sha2(a,0), sha2(b,0) from test;
select sha2(a,224), sha2(b,224) from test;
select sha2(a,256), sha2(b,256) from test;
select sha2(a,384), sha2(b,384) from test;
select sha2(a,512), sha2(b,512) from test;
DROP TABLE IF EXISTS test;

-- test bigint
select sha2('aa',123123131212312313212312123132121313);
select sha2('aa',4294967680);
select sha2('aaaa', 66048);

\c regression
DROP DATABASE IF EXISTS test_sha;
