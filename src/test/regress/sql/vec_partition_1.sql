set current_schema=vector_engine;
set enable_vector_engine=on;

 create schema FVT_DATA_PARTITION;

CREATE TABLE FVT_DATA_PARTITION.partition_wise_join_table_000_3 (
C_CHAR CHAR(1024),
C_VARCHAR VARCHAR(1024),
C_INT INTEGER,
C_DP double precision,
C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE
) with (orientation = column)
partition  by  range(C_INT)
(
  partition partition_wise_join_table_000_3_1  values less than (10),
  partition partition_wise_join_table_000_3_2  values less than (100)
);

insert into FVT_DATA_PARTITION.partition_wise_join_table_000_3 values('abcdefg','abcdedg',5,1,'2011-2-12'),('bcdefgh','bcdefgh',1150,2,'2013-2-12');


create table FVT_DATA_PARTITION.insert_partition_table_001(C_CHAR CHAR(1035),  C_VARCHAR VARCHAR(1035),  C_INT INTEGER,  C_DP double precision, C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE, C_TS timestamp) with (orientation = column)
partition by range (C_TS_WITHOUT,C_TS)
(
     partition insert_partition_table_001_1  values less than ('2010-05-01','2000-05-01') tablespace pg_default,
     partition insert_partition_table_001_2  values less than ('2010-10-01','2000-10-01')
);
insert into FVT_DATA_PARTITION.insert_partition_table_001 values('ABCDEFG','abcdefg',111,1.111,'2010-01-01 01:01:01','2000-01-01 01:01:01');
insert into FVT_DATA_PARTITION.insert_partition_table_001 values('BCDEFGH','bcdefgh',222,2.222,'2010-02-02 02:02:02','2000-02-02 02:02:02');
insert into FVT_DATA_PARTITION.insert_partition_table_001 values('CDEFGHI','cdefghi',333,3.333,'2010-03-03 03:03:03','2000-03-03 03:03:03');
insert into FVT_DATA_PARTITION.insert_partition_table_001 values('DEFGHIJ','defghij',444,4.444,'2010-04-04 04:04:04','2000-04-04 04:04:04');
insert into FVT_DATA_PARTITION.insert_partition_table_001 values('EFGHIJK','efghijk',555,5.555,'2010-05-05 05:05:05','2000-05-05 05:05:05');
insert into FVT_DATA_PARTITION.insert_partition_table_001 values('FGHIJKL','fghijkl',666,6.666,'2010-06-06 06:06:06','2000-06-06 06:06:06');
insert into FVT_DATA_PARTITION.insert_partition_table_001 values('GHIJKLM','ghijklm',777,7.777,'2010-07-07 07:07:07','2000-07-07 07:07:07');
insert into FVT_DATA_PARTITION.insert_partition_table_001 values('HIJKLMN','hijklmn',888,8.888,'2010-08-08 08:08:08','2000-08-08 08:08:08');
insert into FVT_DATA_PARTITION.insert_partition_table_001 values('IJKLMNO','ijklmno',999,9.999,'2010-09-09 09:09:09','2000-09-09 09:09:09');

select count(*) from FVT_DATA_PARTITION.insert_partition_table_001 partition (insert_partition_table_001_1);

select count(*) from FVT_DATA_PARTITION.insert_partition_table_001 partition (insert_partition_table_001_2);
