-- --------------------------------------------------------------
-- rollback pg_catalog.pg_collation
-- --------------------------------------------------------------
delete from pg_catalog.pg_collation where collname='zh_CN' and collnamespace=11 and collencoding=36 and collcollate='zh_CN.gb18030' and collctype='zh_CN.gb18030';
delete from pg_catalog.pg_collation where collname='zh_CN.gb18030' and collnamespace=11 and collencoding=36 and collcollate='zh_CN.gb18030' and collctype='zh_CN.gb18030';