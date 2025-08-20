create schema test_table_filegroup;
set current_schema to test_table_filegroup;
create table t1(a int) on [primary22];
create table t2(a int) on "default";
create table t3(a int) textimage_on [primary22];
create table t4(a int) textimage_on "default";
create table t5(a int) on "default" textimage_on [primary22];
create table t6(a int) on "default" textimage_on "default";
create table t7(a int) textimage_on "default" on "default"; -- d database error
create table t7(a int) textimage_on [primary22] on "default"; -- d database error
create table t7(a int) on commit;
create table t8(a int) textimage_on commit;
create table t9(a int) on commit on "default"; -- d database error
create table t9(a int) on commit delete rows on "default" textimage_on "default"; -- d database error
create table t9(a int) on commit delete rows on "default"; -- d database error
create table t9(a int) on commit delete rows textimage_on "default"; -- d database error
create table t9(a int) on commit textimage_on [primary22];
create table t10(a int) on commit textimage_on [primary22] on "default"; -- d database error
create table t10(a int) textimage_on [primary22] on "default" on commit ; -- d database error
create table t10(id int) on [filegroup];
create table t11(id int) on filegroup;
create table t12(id int) on 'filegroup';
create table t13(id int) on "filegroup";
create table t14(id int) on "filegroup1";
create table t15(id int) on 'default';
create table t16(id int) on "default";
create table t17(id int) on [default];
create table t18(id int) on default; -- d database error, og ok
create table t18(id int) on $[default]; -- d database error
create table t18(id int) on [default; -- d database error
create table t18(id int) on default]; -- d database error
create table t19(id int) on '123filegroup';
create table t20(id int) on '_123filegroup';
create table t21(id int) on table;
create table t22(id int) on 'table';
create table t23(id int) on "table";
create table t24(id int) on [table];
create table t25(id int) on 'New FileGroup';
create table t26(id int) on [New FileGroup]; --og error
create table t26(id int) on [NewFileGroup];

create table test_with_1(a int, CONSTRAINT PK_test_with_1 PRIMARY KEY(a) WITH (PAD_INDEX = OFF) on [primary22]) on [primary22];
create table test_with_2(a int, CONSTRAINT PK_test_with_2 PRIMARY KEY(a)
WITH (PAD_INDEX = ON, FILLFACTOR = 10, IGNORE_DUP_KEY = on, STATISTICS_NORECOMPUTE = on, STATISTICS_INCREMENTAL = on,
ALLOW_ROW_LOCKS = on, ALLOW_PAGE_LOCKS = on, OPTIMIZE_FOR_SEQUENTIAL_KEY = on, XML_COMPRESSION = on)
    ON [primary22])
    ON [primary22];
\d+ test_with_2
create table test_with_3(a int, CONSTRAINT PK_test_with_3 PRIMARY KEY(a)
WITH (PAD_INDEX = OFF, FILLFACTOR = 50, IGNORE_DUP_KEY = off, STATISTICS_NORECOMPUTE = off, STATISTICS_INCREMENTAL = off,
ALLOW_ROW_LOCKS = off, ALLOW_PAGE_LOCKS = off, OPTIMIZE_FOR_SEQUENTIAL_KEY = off, XML_COMPRESSION = off)
    ON [primary22])
    ON [primary22];
create table test_with_4(a int, CONSTRAINT PK_test_with_4 PRIMARY KEY(a) with (COMPRESSION_DELAY = 0 ));
create table test_with_5(a int, CONSTRAINT PK_test_with_5 PRIMARY KEY(a) with (COMPRESSION_DELAY = 0 Minutes));
create table test_with_6(a int, CONSTRAINT PK_test_with_6 PRIMARY KEY(a) with (COMPRESSION_DELAY = 0 MINUTES));
create table test_with_7(a int, CONSTRAINT PK_test_with_7 PRIMARY KEY(a) with (COMPRESSION_DELAY = 0 MiNUtes));
create table test_with_8(a int, CONSTRAINT PK_test_with_8 PRIMARY KEY(a) with (COMPRESSION_DELAY = 5 ));
create table test_with_9(a int, CONSTRAINT PK_test_with_9 PRIMARY KEY(a) with (COMPRESSION_DELAY = 10 minutes));
create table test_with_10(a int, CONSTRAINT PK_test_with_10 PRIMARY KEY(a) with (COMPRESSION_DELAY = 20 minute));
create table test_with_11(a int, CONSTRAINT PK_test_with_11 PRIMARY KEY(a) with (COMPRESSION_DELAY = 30 MiNUtes));
create table test_with_12(a int, CONSTRAINT PK_test_with_12 PRIMARY KEY(a) with (COMPRESSION_DELAY = 10080 minute));
create table test_with_13(a int, CONSTRAINT PK_test_with_13 PRIMARY KEY(a) with (COMPRESSION_DELAY = 10081 MiNUtes)); --error, compression_delay [0, 10080] int in d database
create table test_with_13(a int, CONSTRAINT PK_test_with_13 PRIMARY KEY(a) with (COMPRESSION_DELAY = -1 minutes)); -- d database syntax error
create table test_with_13(a int, CONSTRAINT PK_test_with_13 PRIMARY KEY(a) with (COMPRESSION_DELAY = 5.0 MiNUtes)); --error
create table test_with_13(a int, CONSTRAINT PK_test_with_13 PRIMARY KEY(a) with (COMPRESSION_DELAY = 30 minute, pad_index = 1));
create table test_with_14(a int, CONSTRAINT PK_test_with_14 PRIMARY KEY(a) with (COMPRESSION_DELAY = 30 minute, pad_index = n));
create table test_with_15(a int, CONSTRAINT PK_test_with_15 PRIMARY KEY(a) with (COMPRESSION_DELAY = 10080 minutes, pad_index = y));
create table test_with_16(a int, CONSTRAINT PK_test_with_16 PRIMARY KEY(a) with (DATA_COMPRESSION = none));
create table test_with_17(a int, CONSTRAINT PK_test_with_17 PRIMARY KEY(a) with (DATA_COMPRESSION = 'none')); -- d database error, og ok
create table test_with_18(a int, CONSTRAINT PK_test_with_18 PRIMARY KEY(a) with (DATA_COMPRESSION = row));
create table test_with_19(a int, CONSTRAINT PK_test_with_19 PRIMARY KEY(a) with (data_compression = page));
create table test_with_20(a int, CONSTRAINT PK_test_with_20 PRIMARY KEY(a) with (data_compression = COLUMNSTORE));
create table test_with_21(a int, CONSTRAINT PK_test_with_21 PRIMARY KEY(a) with (data_compression = COLUMNSTORE_ARCHIVE));
create table test_with_22(a int, CONSTRAINT PK_test_with_22 PRIMARY KEY(a) with (data_compression = error_option)); --error
create table test_with_22(a int, CONSTRAINT PK_test_with_22 PRIMARY KEY(a) with (data_compression = none, pad_index = on, pad_index = off)); --error
create table test_with_22(a int, CONSTRAINT PK_test_with_22 PRIMARY KEY(a) with (pad_index = yes, COMPRESSION_DELAY = 0 MiNUtes))
with(fillfactor = 20, storage_type = astore);
create table test_with_23(a int, CONSTRAINT PK_test_with_23 PRIMARY KEY(a) with (data_compression = none, pad_index = on, COMPRESSION_DELAY = 0 MiNUtes))
with(fillfactor = 20, storage_type = astore);
create table test_with_24(a int, CONSTRAINT PK_test_with_24 PRIMARY KEY(a) with (data_compression = row, pad_index = on, COMPRESSION_DELAY = 0))
with(fillfactor = 20, storage_type = ustore);
create table test_with_25(a int, CONSTRAINT PK_test_with_25 PRIMARY KEY(a) with (pad_index = on, COMPRESSION_DELAY = 0, storage_type = ustore))
with(fillfactor = 20, storage_type = ustore);
create table test_with_26(a int, PRIMARY KEY(a) with (pad_index = on, COMPRESSION_DELAY = 0, storage_type = ustore))
with(fillfactor = 20, storage_type = ustore);
create table test_with_27(a int, PRIMARY KEY(a) with (pad_index = on, fillfactor = -1)); --error, fillfactor [1, 100] int in d database
create table test_with_27(a int, PRIMARY KEY(a) with (pad_index = on, fillfactor = 1));
\d+ test_with_27
create table test_with_28(a int, PRIMARY KEY(a) with (pad_index = on, fillfactor = 50));
create table test_with_29(a int, PRIMARY KEY(a) with (pad_index = on, fillfactor = 100));
create table test_with_30(a int, PRIMARY KEY(a) with (pad_index = on, fillfactor = 105)); --error
create table test_with_30(a int, PRIMARY KEY(a) with (pad_index = on, fillfactor = 50.2)); --error
create table test_with_30(a int, PRIMARY KEY(a) with (pad_index = on, fillfactor = 50) on [primary]);
create table test_with_31(a int primary key with (pad_index = on, fillfactor = 50) on [primary]);
create table test_with_32(a int, unique(a) with (pad_index = on, fillfactor = 50) on [primary]);
create table test_with_33(a int unique with (pad_index = on, fillfactor = 50) on [primary]);
create table test_with_34(id int primary key with(fillfactor = 20), name varchar(50));
alter table test_with_34 add constraint unique_name unique(name) with (fillfactor = 50) on [primary];
create table test_with_35(id int, name varchar(50));
alter table test_with_35 add constraint pk_id primary key(id) with (fillfactor = 50) on [primary];

drop schema test_table_filegroup cascade;
