--This file is base test of json and jsonb.

select oid,* from pg_type where typname like 'json%' order by oid;
select oid,* from pg_proc where proname like '%json%' order by oid;
select * from pg_aggregate where aggfnoid in (3124, 3403) order by aggfnoid;
select oid, * from pg_operator where oprleft in (114, 3802) order by oid;
select oid, * from pg_opfamily where oid in (4033, 4034, 4035, 4036, 4037) order by oid;
select * from pg_cast where castsource in (114, 3802) order by castsource, casttarget;
select * from pg_opclass where opcfamily in (4033, 4034, 4035, 4036, 4037) order by opcfamily;
select * from pg_amproc where amprocfamily in (4033, 4034, 4035, 4036, 4037) order by amprocfamily, amprocnum;
select * from pg_amop where amopfamily in (4033, 4034, 4035, 4036, 4037) order by amopfamily, amopstrategy;
