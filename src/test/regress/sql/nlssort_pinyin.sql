create schema nlssort_pinyin_schema;
set search_path = nlssort_pinyin_schema;

-- test null
select nlssort(NULL, 'nls_sort=schinese_pinyin_m');
select nlssort('', NULL);
select nlssort(NULL, NULL);

-- test wrong parameter
select nlssort('', ' nls_sort =  schinese_pinyin_m  ');
select nlssort('', ' nls_sort =  generic_m_ci  ');
select nlssort('', 'nls_sort=s chinese_pinyin_m');
select nlssort('', 'nls_sort=g eneric_m_ci');
select nlssort('', 'nls_sort=schinese');
select nlssort('', 'nls_sort=generic');

-- test single char nlssort code
select nlssort('', 'nls_sort=schinese_pinyin_m');
select nlssort('', 'nls_sort=schinese_pinyin_m');
select nlssort('$', 'nls_sort=schinese_pinyin_m');
select nlssort('&', 'nls_sort=schinese_pinyin_m');
select nlssort('''', 'nls_sort=schinese_pinyin_m');
select nlssort('0', 'nls_sort=schinese_pinyin_m');
select nlssort('A', 'nls_sort=schinese_pinyin_m');
select nlssort('\', 'nls_sort=schinese_pinyin_m');
select nlssort('a', 'nls_sort=schinese_pinyin_m');
select nlssort('倰', 'nls_sort=schinese_pinyin_m');
select nlssort('冔', 'nls_sort=schinese_pinyin_m');
select nlssort('勆', 'nls_sort=schinese_pinyin_m');
select nlssort('', 'nls_sort=schinese_pinyin_m');
select nlssort('「', 'nls_sort=schinese_pinyin_m');
select nlssort('★', 'nls_sort=schinese_pinyin_m');
select nlssort('ⅰ', 'nls_sort=schinese_pinyin_m');
select nlssort('⒈', 'nls_sort=schinese_pinyin_m');
select nlssort('⑴', 'nls_sort=schinese_pinyin_m');
select nlssort('①', 'nls_sort=schinese_pinyin_m');
select nlssort('㈠', 'nls_sort=schinese_pinyin_m');
select nlssort('Ⅰ', 'nls_sort=schinese_pinyin_m');
select nlssort('Ⅴ', 'nls_sort=schinese_pinyin_m');
select nlssort('', 'nls_sort=schinese_pinyin_m');
select nlssort('０', 'nls_sort=schinese_pinyin_m');
select nlssort('Ａ', 'nls_sort=schinese_pinyin_m');
select nlssort('ａ', 'nls_sort=schinese_pinyin_m');
select nlssort('ぎ', 'nls_sort=schinese_pinyin_m');
select nlssort('ガ', 'nls_sort=schinese_pinyin_m');
select nlssort('α', 'nls_sort=schinese_pinyin_m');
select nlssort('猋', 'nls_sort=schinese_pinyin_m');
select nlssort('珬', 'nls_sort=schinese_pinyin_m');
select nlssort('甂', 'nls_sort=schinese_pinyin_m');
select nlssort('Ꮬ', 'nls_sort=schinese_pinyin_m');
select nlssort('ᴂ', 'nls_sort=schinese_pinyin_m');
select nlssort('겷', 'nls_sort=schinese_pinyin_m');
select nlssort('뛑', 'nls_sort=schinese_pinyin_m');
select nlssort('', 'nls_sort=schinese_pinyin_m');
select nlssort('𡤝', 'nls_sort=schinese_pinyin_m');
select nlssort('𦪫', 'nls_sort=schinese_pinyin_m');
select nlssort('𰀅', 'nls_sort=schinese_pinyin_m');

select nlssort('', 'nls_sort=generic_m_ci');
select nlssort('', 'nls_sort=generic_m_ci');
select nlssort('$', 'nls_sort=generic_m_ci');
select nlssort('&', 'nls_sort=generic_m_ci');
select nlssort('''', 'nls_sort=generic_m_ci');
select nlssort('0', 'nls_sort=generic_m_ci');
select nlssort('A', 'nls_sort=generic_m_ci');
select nlssort('\', 'nls_sort=generic_m_ci');
select nlssort('a', 'nls_sort=generic_m_ci');
select nlssort('倰', 'nls_sort=generic_m_ci');
select nlssort('冔', 'nls_sort=generic_m_ci');
select nlssort('勆', 'nls_sort=generic_m_ci');
select nlssort('', 'nls_sort=generic_m_ci');
select nlssort('「', 'nls_sort=generic_m_ci');
select nlssort('★', 'nls_sort=generic_m_ci');
select nlssort('ⅰ', 'nls_sort=generic_m_ci');
select nlssort('⒈', 'nls_sort=generic_m_ci');
select nlssort('⑴', 'nls_sort=generic_m_ci');
select nlssort('①', 'nls_sort=generic_m_ci');
select nlssort('㈠', 'nls_sort=generic_m_ci');
select nlssort('Ⅰ', 'nls_sort=generic_m_ci');
select nlssort('Ⅴ', 'nls_sort=generic_m_ci');
select nlssort('', 'nls_sort=generic_m_ci');
select nlssort('０', 'nls_sort=generic_m_ci');
select nlssort('Ａ', 'nls_sort=generic_m_ci');
select nlssort('ａ', 'nls_sort=generic_m_ci');
select nlssort('ぎ', 'nls_sort=generic_m_ci');
select nlssort('ガ', 'nls_sort=generic_m_ci');
select nlssort('α', 'nls_sort=generic_m_ci');
select nlssort('猋', 'nls_sort=generic_m_ci');
select nlssort('珬', 'nls_sort=generic_m_ci');
select nlssort('甂', 'nls_sort=generic_m_ci');
select nlssort('Ꮬ', 'nls_sort=generic_m_ci');
select nlssort('ᴂ', 'nls_sort=generic_m_ci');
select nlssort('겷', 'nls_sort=generic_m_ci');
select nlssort('뛑', 'nls_sort=generic_m_ci');
select nlssort('', 'nls_sort=generic_m_ci');
select nlssort('𡤝', 'nls_sort=generic_m_ci');
select nlssort('𦪫', 'nls_sort=generic_m_ci');
select nlssort('𰀅', 'nls_sort=generic_m_ci');

-- test multi chars nlssort code
select nlssort('       ', 'nls_sort=schinese_pinyin_m');
select nlssort('AbC啊  ', 'nls_sort=schinese_pinyin_m');
select nlssort('AbC 啊  ', 'nls_sort=schinese_pinyin_m');
select nlssort('  AbC啊  ', 'nls_sort=schinese_pinyin_m');

select nlssort('       ', 'nls_sort=generic_m_ci');
select nlssort('AbC啊  ', 'nls_sort=generic_m_ci');
select nlssort('AbC 啊  ', 'nls_sort=generic_m_ci');
select nlssort('  AbC啊  ', 'nls_sort=generic_m_ci');

-- test nlssort func user in order by statement
drop table if exists tb_test;
create table tb_test(c1 text);

insert into tb_test values('');
insert into tb_test values('');
insert into tb_test values('$');
insert into tb_test values('&');
insert into tb_test values('''');
insert into tb_test values('0');
insert into tb_test values('A');
insert into tb_test values('\');
insert into tb_test values('a');
insert into tb_test values('倰');
insert into tb_test values('冔');
insert into tb_test values('勆');
insert into tb_test values('');
insert into tb_test values('「');
insert into tb_test values('★');
insert into tb_test values('ⅰ');
insert into tb_test values('⒈');
insert into tb_test values('⑴');
insert into tb_test values('①');
insert into tb_test values('㈠');
insert into tb_test values('Ⅰ');
insert into tb_test values('Ⅴ');
insert into tb_test values('');
insert into tb_test values('０');
insert into tb_test values('Ａ');
insert into tb_test values('ａ');
insert into tb_test values('ぎ');
insert into tb_test values('ガ');
insert into tb_test values('α');
insert into tb_test values('猋');
insert into tb_test values('珬');
insert into tb_test values('甂');
insert into tb_test values('Ꮬ');
insert into tb_test values('ᴂ');
insert into tb_test values('겷');
insert into tb_test values('뛑');
insert into tb_test values('');
insert into tb_test values('𡤝');
insert into tb_test values('𦪫');
insert into tb_test values('𰀅');
insert into tb_test values('       ');
insert into tb_test values('AbC啊  ');
insert into tb_test values('AbC 啊  ');
insert into tb_test values('  AbC啊  ');

select c1,  nlssort(c1, 'nls_sort=schinese_pinyin_m') from tb_test order by nlssort(c1, 'nls_sort=schinese_pinyin_m');
select c1,  nlssort(c1, 'nls_sort=generic_m_ci') from tb_test order by nlssort(c1, 'nls_sort=generic_m_ci');

-- test nlssort func used in procedure (compilation should not report errors)
drop table if exists tb_test;
create table tb_test(col1 varchar2);
create or replace package pckg_test as
procedure proc_test(i_col1 in varchar2);
function func_test(i_col1 in varchar2) return varchar2;
end pckg_test;
/

create or replace package body pckg_test as
procedure proc_test(i_col1 in varchar2) as
v_a varchar2;
v_b varchar2;
begin
if func_test(i_col1) < func_test('阿') then
v_a:= func_test(i_col1);
end if;
select nlssort(col1,'NLS_SORT=SCHINESE_PINYIN_M') into v_b from tb_test where col1=i_col1;
end;
function func_test(i_col1 in varchar2) return varchar2 as
begin
return nlssort(i_col1,'NLS_SORT=SCHINESE_PINYIN_M');
end;
end pckg_test;
/

-- It will core when the length of the first argument is 0.
-- ORA compatibility mode treats "" as null, so test it in MySQL compatibility mode.
create database b_dbcompatibility TEMPLATE=template0 dbcompatibility='B';
\c b_dbcompatibility
set client_encoding=utf8;

select nlssort('', 'nls_sort=schinese_pinyin_m');

\c regression
clean connection to all force for database b_dbcompatibility;
drop database b_dbcompatibility;

-- test nlssort is shippable or not
\sf nlssort

drop schema nlssort_pinyin_schema cascade;