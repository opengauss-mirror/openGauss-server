\setrandom aid 1 1000
\setrandom anum 1 1000
START TRANSACTION;
insert into lightcn_t1 values (:aid, :anum);
select * from lightcn_t1 where id = :aid;
update lightcn_t1 set num = num+1 where id = :aid;
delete from lightcn_t1 where id = :aid;
COMMIT TRANSACTION;
