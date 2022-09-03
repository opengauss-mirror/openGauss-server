-- disable the gtt function in the ustore.
-- testcase 1
set enable_default_ustore_table = off;
show enable_default_ustore_table;
create global temp table t1 (id int,name varchar(10)) with (storage_type = ustore);
reset enable_default_ustore_table;
-- testcase 2
set enable_default_ustore_table = on;
show enable_default_ustore_table;
create global temp table t1 (id int,name varchar(10));
reset enable_default_ustore_table;
