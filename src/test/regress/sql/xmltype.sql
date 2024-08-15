\df xmltype
\sf xmltype
select * from pg_type where typname = 'xmltype';
create table t_xmltype(xml xmltype);
\d t_xmltype
-- 使用xmltype函数生成数据
select XMLTYPE('<Config id="5">XML</Config>');
select xmltype('<Config id="5">XML</Config>');
insert into t_xmltype values(XMLTYPE('<Config id="5">XML</Config>'));
insert into t_xmltype values(xmltype('<Config id="5">XML</Config>'));
select * from t_xmltype;
drop table t_xmltype;

-- test xmltype functions
create table test_xmltype (
    id int,
    name varchar2(4000),
    data xmltype
);
-- 插入正确xmltype
Insert INTO test_xmltype
VALUES (1,'test xml doc','<?xml version="1.0" encoding="UTF-8" ?>
<collection xmlns="">
<record>
<leader>-----nam0-22-----^^^450-</leader>
<datafield tag="200" ind1="1" ind2=" ">
<subfield code="a">抗震救灾</subfield>
<subfield code="f">奥运会</subfield>
</datafield>
<datafield tag="209" ind1=" " ind2=" ">
<subfield code="a">经济学</subfield>
<subfield code="b">计算机</subfield>
<subfield code="c">10001</subfield>
<subfield code="d">2005-07-09</subfield>
</datafield>
<datafield tag="610" ind1="0" ind2=" ">
<subfield code="a">计算机</subfield>
<subfield code="a">笔记本</subfield>
</datafield>
</record>
</collection>') ;
-- 使用createXML函数插入
Insert INTO test_xmltype
VALUES (2,'test xml doc',xmlType.createXML('<?xml version="1.0" encoding="UTF-8" ?>
<collection xmlns="">
<record>
<leader>-----nam0-22-----^^^450-</leader>
<datafield tag="200" ind1="1" ind2=" ">
<subfield code="a">抗震救灾</subfield>
<subfield code="f">奥运会</subfield>
</datafield>
<datafield tag="209" ind1=" " ind2=" ">
<subfield code="a">经济学</subfield>
<subfield code="b">计算机</subfield>
<subfield code="c">10001</subfield>
<subfield code="d">2005-07-09</subfield>
</datafield>
<datafield tag="610" ind1="0" ind2=" ">
<subfield code="a">计算机</subfield>
<subfield code="a">笔记本</subfield>
</datafield>
</record>
</collection>')) ;
-- 使用xmltype插入
Insert INTO test_xmltype
VALUES (3,'test xml doc',xmltype('<?xml version="1.0" encoding="UTF-8" ?>
<collection xmlns="">
<record>
<leader>-----nam0-22-----^^^450-</leader>
<datafield tag="209" ind1=" " ind2=" ">
<subfield code="a">经济学</subfield>
<subfield code="b">计算机</subfield>
<subfield code="c">10001</subfield>
<subfield code="d">2005-07-09</subfield>
</datafield>
</record>
</collection>')) ;
select *from test_xmltype;
-- xmltype functions 
-- extract
select extract(x.data,'/collection/record/datafield/subfield') xmlseq from test_xmltype x;
select extract(x.data,'/collection/record/datafield') xmlseq from test_xmltype x;
-- 传入空字符串
select extract(x.data,'') xmlseq from test_xmltype x;
-- 返回空集合
select extract(x.data,'/collection/records') xmlseq from test_xmltype x;
-- XMLSequence
select XMLSequence(extract(x.data,'/collection/record/datafield')) xmlseq from test_xmltype x;
select unnest(XMLSequence(extract(x.data,'/collection/record/datafield/subfield'))) xmlseq from test_xmltype x;
-- extractvalue
select extractvalue(x.data,'/collection/record/leader') as A from test_xmltype x;
select extractvalue(x.data,'/collection/record/datafield[@tag="209"]/subfield[@code="a"]') as A from test_xmltype x;
-- 多节点报错
select extractvalue(x.data,'/collection/record/datafield/subfield') as A from test_xmltype x;

-- getStringVal
select getStringVal(extract(x.data,'/collection/record/datafield/subfield')) a from test_xmltype x;
select getStringVal(extract(x.data,'/collection/record/datafield')) a from test_xmltype x;

-- existsnode
select existsnode(x.data,'/collection/record/datafield[@tag="209"]/subfield[@code="f"]') as a from test_xmltype x;
select existsnode(x.data,'/collection/record/datafield[@tag="209"]/subfield[@code="a"]') as a from test_xmltype x;

-- appendchildxml 函数
-- 在record节点后新增一个record节点
select getStringVal( extract(data,'/collection')) as data from test_xmltype;
update test_xmltype set data=appendchildxml(data,'/collection',xmltype('<record>
<leader>-----nam0-23-----^^^500-</leader>
<datafield tag="300" ind1="1" ind2=" ">
<subfield code="b">抗震救灾</subfield>
<subfield code="h">奥运会</subfield>
</datafield>
<datafield tag="301" ind1=" " ind2=" ">
<subfield code="a">英语</subfield>
<subfield code="b">数学</subfield>
<subfield code="c">10001</subfield>
<subfield code="d">2022-11-02</subfield>
</datafield>
<datafield tag="302" ind1="0" ind2=" ">
<subfield code="a">计算机科学</subfield>
<subfield code="a">电脑</subfield>
</datafield>
</record>'));
-- 查看是否插入成功
select getStringVal( extract(data,'/collection')) as data from test_xmltype;
DELETE from test_xmltype;
insert into test_xmltype values(1,'test xml',
xmlType('<?xml version="1.0" encoding="utf-8" ?>
<!--this is a booklist-->
<booklist type="C语言">
 <book category="computer">
  <title>learning computer</title>
  <author>July</author>
  <pageNumber>600</pageNumber>
 </book>
</booklist>'));
select * from test_xmltype;
-- 在book节点下新增一个元素
update test_xmltype set data=appendchildxml(data,'/booklist/book',XMLType('<price>82</price>'));
select getStringVal( extract(data,'/booklist')) as data from test_xmltype;
--在book节点下新增一个时间类型
update test_xmltype set data=appendchildxml(data,'/booklist/book',XMLType('<publication>2010-12-10 14:30:00</publication>'));
select getStringVal( extract(data,'/booklist')) as data from  test_xmltype;
--在book节点下新增一个浮点类型
update test_xmltype set data=appendchildxml(data,'/booklist/book',XMLType('<price>100.23</price>'));
select getStringVal( extract(data,'/booklist')) as data from  test_xmltype;
--在book节点下新增一个字符类型
update test_xmltype set data=appendchildxml(data,'/booklist/book',XMLType('<press>人民出版社</press>'));
select getStringVal( extract(data,'/booklist')) as data from  test_xmltype;
select extractvalue(data,'/booklist/book/press') from  test_xmltype;
--在book节点下新增一个空
update test_xmltype set data=appendchildxml(data,'/booklist/book',XMLType('<testa>null</testa>'));
update test_xmltype set data=appendchildxml(data,'/booklist/book',XMLType('<test>''</test>'));
update test_xmltype set data=appendchildxml(data,'/booklist/book',XMLType('<press></press>'));
select getStringVal( extract(data,'/booklist')) as data from  test_xmltype;
-- namespace合法
update test_xmltype set data=appendchildxml(data,'/Book',xmltype('<Pagenumber>100</Pagenumber>'),array[array['lib','http://www.library.com']] );
select getStringVal( extract(data,'/Book')) as data from  test_xmltype;
-- namespace不合法
--为空
update test_xmltype set data=appendchildxml(data,'/Book',xmltype('<Pagenumber>100</Pagenumber>'),'');
update test_xmltype set data=appendchildxml(data,'/Book',xmltype('<Pagenumber>100</Pagenumber>'),null);
--为其他字符
update test_xmltype set data=appendchildxml(data,'/Book',xmltype('<Pagenumber>100</Pagenumber>'),'test');
--命名空间格式有误
update test_xmltype set data=appendchildxml(data,'/Book',xmltype('<Pagenumber>100</Pagenumber>'),array['lib','http://www.library.com'] );
-- 强制转换为XML
update test_xmltype set data=appendchildxml(data,'/menu/beers','<name>Car</name>'::xml) where id=1;
update test_xmltype set data=appendchildxml(data,'/Book','<Pagenumber>100</Pagenumber>'::xml,array[array['lib','http://www.library.com']] ) where id=3;
select * from test_xmltype where id=1;
select * from test_xmltype where id=3;
-- 中文测试
drop table IF EXISTS  test_xmltype;
create table test_xmltype(
id int,
name text,
date_time date,
data xmltype);
--插入数据
insert into test_xmltype values(1,'test xml',sysdate,
xmlType('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<资料 xmlns:IT="http://www.lenovo.com" xmlns:建筑="myURN:中联">
   <设备 IT:编号="联想6515b" 建筑:编号="中联F001">
      <IT:设备名>笔记本</IT:设备名>
      <IT:生产商>联想集团</IT:生产商>
      <IT:地址>北京市中关村127号</IT:地址>
      <建筑:设备名>起重机</建筑:设备名>
      <建筑:生产商>中联重科</建筑:生产商>
      <建筑:地址>湖南省长沙市新开铺113号</建筑:地址>
   </设备>
</资料>'));
update test_xmltype set data=appendchildxml(data,'/资料/设备',XMLType('<生厂商联系方式>010-234567</生厂商联系方式>'));
--查看是否追加成功
select getStringVal( extract(data,'/资料')) as data from  test_xmltype;
drop table test_xmltype;
-- 其他兼容性测试
create database db_postgres dbcompatibility = 'PG';
\c db_postgres
create table test_xmltype(
id int,
name text,
data xmltype);
Insert INTO test_xmltype
VALUES (1,'test xml doc','<?xml version="1.0" encoding="UTF-8" ?>
<collection xmlns="">
<record>
<leader>-----nam0-22-----^^^450-</leader>
<datafield tag="200" ind1="1" ind2=" ">
<subfield code="a">抗震救灾</subfield>
<subfield code="f">奥运会</subfield>
</datafield>
<datafield tag="209" ind1=" " ind2=" ">
<subfield code="a">经济学</subfield>
<subfield code="b">计算机</subfield>
<subfield code="c">10001</subfield>
<subfield code="d">2005-07-09</subfield>
</datafield>
<datafield tag="610" ind1="0" ind2=" ">
<subfield code="a">计算机</subfield>
<subfield code="a">笔记本</subfield>
</datafield>
</record>
</collection>') ;
select extract(x.data,'/collection/record/datafield/subfield') xmlseq from test_xmltype x;
select XMLSequence(extract(x.data,'/collection/record/datafield/subfield')) xmlseq from test_xmltype x;
select extractvalue(x.data,'/collection/record/leader') as A from test_xmltype x;
select getStringVal(extract(x.data,'/collection/record/datafield/subfield')) a from test_xmltype x;
select existsnode(x.data,'/collection/record/datafield[@tag="209"]/subfield[@code="a"]') as a from test_xmltype x;
drop table test_xmltype;
\c regression
drop database db_postgres;