set current_schema='shipping_schema';

---------------current_user/getpgusername shipping case1-----------------
---case1 shipping---
explain (num_costs off) select current_user, getpgusername(), sum(t4.a) from t4, t5 where t4.b = t5.a; 

---case2 shipping---
explain (num_costs off) select sum(t4.a) from t4, t5 where t4.b = t5.a and t5.c = current_user or t5.c = getpgusername();

select sum(t4.a) from t4, t5 where t4.b = t5.a and t5.c = current_user or t5.c = getpgusername();

---case3 shipping---
explain (num_costs off) select current_user,t4.a from t4;
explain (num_costs off) select current_user, getpgusername(), t4.a from t4 order by 1,2,3 limit 1;



-------------current_scheam/session_user/current_databse/pg_client_encoding ------------
---case1 shipping---
explain (costs off) select current_schema, session_user, current_database() ,pg_client_encoding() ,
sum(t4.a) from t4, t5 where t4.b = t5.a;

---case2 shipping---
explain (costs off) select sum(t4.a) from t4, t5 where t4.b = t5.a and 
session_user = current_user or current_database() = current_schema and pg_client_encoding()='UTF-8';

select sum(t4.a) from t4, t5 where t4.b = t5.a and 
session_user = current_user or current_database() = current_schema and pg_client_encoding()='UTF-8';

------timenow/current_timestamp/sysdate/statement_timestamp/pg_systimestamp---
---case1 shipping---

explain (costs off) select timenow(), current_timestamp, sysdate,statement_timestamp() ,pg_systimestamp(),
t4.a from t4, t5 where t4.b = t5.a;

----case2 shipping---
explain (costs off) select t4.a from t4, t5 where t4.b = t5.a and timenow()::timestamp(0)=sysdate::timestamp(0)
and pg_systimestamp()::timestamp(0)= statement_timestamp()::timestamp(0) and current_timestamp < '9999-12-01';



