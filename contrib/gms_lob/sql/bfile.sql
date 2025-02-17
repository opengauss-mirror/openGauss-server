create extension gms_lob;
create extension gms_output;
CREATE or REPLACE DIRECTORY bfile_test_dir AS '/tmp';
create table falt_bfile (id number, bfile_name bfile);
insert into falt_bfile values(1, bfilename('bfile_test_dir','regress_bfile.txt'));
copy (select * from falt_bfile) to '/tmp/regress_bfile.txt';
select gms_output.enable;

---------bfilename函数-------------------
--测试参数为空值
select bfilename('', 'a');
select bfilename('a', '');

--测试参数带空格
select bfilename('sdf sfa', 'as df .txt');

---------bfilein函数---------------------
--测试参数为空值
select bfilein('');

--测试参数格式不对
select bfilein('bfilenamell(sdf,asdf)');

--测试参数格式带空格
select bfilein(' bfilename ( sdf , asdf ) ');

--正常值测试
select bfilein('bfilename(bfile_test_dir,regress_bfile.txt)');

---------bfileout函数---------------------
--测试参数为空值
select bfileout('');

--正常值测试
select bfileout(bfilename('bfile_test_dir','regress_bfile.txt'));

---------bfilerecv函数---------------------
--测试参数为空值
select bfilerecv('');

---------bfilesend函数---------------------
--测试参数为空值
select bfilesend('');

--正常值测试
select bfilesend(bfilename('bfile_test_dir','regress_bfile.txt'));

---------bfileopen函数----------------------
--测试参数为空值
select gms_lob.bfileopen('', 0);

--无效路径
select gms_lob.bfileopen(bfilename('bfile_test_dir','bfile_test.txt'), 0);

---------bfileclose函数---------------------
--测试参数为空值
select gms_lob.bfileclose('');

--关闭未打开的bfile对象文件
select gms_lob.bfileclose(bfilename('bfile_test_dir','regress_bfile.txt'));

--读取文件偏移量大于文件大小
DECLARE
    buff raw(2000);
    my_bfile bfile;
    amount integer;
    f_offset integer := 1;
BEGIN
    my_bfile := bfilename('bfile_test_dir','regress_bfile.txt');
    gms_lob.fileopen(my_bfile, 0);
    amount := gms_lob.getlength(my_bfile);
    f_offset := amount + f_offset;
    gms_lob.read(my_bfile, amount, f_offset, buff);
    gms_lob.fileclose(my_bfile);
    gms_output.put_line(CONVERT_FROM(decode(buff,'hex'), 'SQL_ASCII'));
END;
/

--读取文件偏移量小于1
DECLARE
    buff raw(2000);
    my_bfile bfile;
    amount integer;
    f_offset integer := 0;
BEGIN
    my_bfile := bfilename('bfile_test_dir','regress_bfile.txt');
    gms_lob.fileopen(my_bfile, 0);
    amount := gms_lob.getlength(my_bfile);
    gms_lob.read(my_bfile, amount, f_offset, buff);
    gms_lob.fileclose(my_bfile);
    gms_output.put_line(CONVERT_FROM(decode(buff,'hex'), 'SQL_ASCII'));
END;
/

--读取的数据量小于1字节
DECLARE
    buff raw(2000);
    my_bfile bfile;
    amount integer := 0;
    f_offset integer := 1;
BEGIN
    my_bfile := bfilename('bfile_test_dir','regress_bfile.txt');
    gms_lob.fileopen(my_bfile, 0);
    gms_lob.read(my_bfile, amount, f_offset, buff);
    gms_lob.fileclose(my_bfile);
    gms_output.put_line(CONVERT_FROM(decode(buff,'hex'), 'SQL_ASCII'));
END;
/

--读取的数据量大于1MB
DECLARE
    buff raw(2000);
    my_bfile bfile;
    amount integer := 1024 * 1024 + 1;
    f_offset integer := 1;
BEGIN
    my_bfile := bfilename('bfile_test_dir','regress_bfile.txt');
    gms_lob.fileopen(my_bfile, 0);
    gms_lob.read(my_bfile, amount, f_offset, buff);
    gms_lob.fileclose(my_bfile);
    gms_output.put_line(CONVERT_FROM(decode(buff,'hex'), 'SQL_ASCII'));
END;
/

--正常读取
DECLARE
    buff raw(2000);
    my_bfile bfile;
    amount integer;
    f_offset integer := 1;
BEGIN
    my_bfile := bfilename('bfile_test_dir','regress_bfile.txt');
    gms_lob.fileopen(my_bfile, 0);
    amount := gms_lob.getlength(my_bfile);
    gms_lob.read(my_bfile, amount, f_offset, buff);
    gms_lob.fileclose(my_bfile);
    gms_output.put_line(CONVERT_FROM(decode(buff,'hex'), 'SQL_ASCII'));
END;
/

drop table falt_bfile;
drop  DIRECTORY bfile_test_dir;
drop extension gms_output;
drop extension gms_lob;