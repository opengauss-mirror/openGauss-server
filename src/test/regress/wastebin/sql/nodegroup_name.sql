CREATE DATABASE "a$_$_GBK_TD" WITH TEMPLATE = template0  ENCODING= 'UTF8' dbcompatibility = 'TD' LC_CTYPE = 'C' lc_collate= 'C';
\c "a$_$_GBK_TD"
create node group 《aa》 with(datanode1);
create node group "《aa》" with(datanode1);
create node group 多字节aa with(datanode1);
create node group "多字节aa" with(datanode1);
create node group <aa with(datanode1);
create node group "<aa" with(datanode1);
create node group aa with(datanode1);
create node group "a123456789a123456789a123456789a123456789a123456789a123456789aa字" with(datanode1);
select group_name,group_members,is_installation from pgxc_group order by group_name;
\c postgres
select group_name,group_members,is_installation from pgxc_group order by group_name;
drop node group <aa;
drop node group "<aa";
drop node group "a123456789a123456789a123456789a123456789a123456789a123456789aa字";
select group_name,group_members,is_installation from pgxc_group order by group_name;

