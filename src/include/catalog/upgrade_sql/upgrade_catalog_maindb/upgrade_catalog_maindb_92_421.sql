-- --------------------------------------------------------------
-- upgrade pg_catalog.pg_collation
-- --------------------------------------------------------------
insert into pg_catalog.pg_collation values ('zh_CN', 11, 10, 36, 'zh_CN.gb18030', 'zh_CN.gb18030'), ('zh_CN.gb18030', 11, 10, 36, 'zh_CN.gb18030', 'zh_CN.gb18030');