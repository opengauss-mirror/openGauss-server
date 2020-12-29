--create resource pool success.
create resource pool Test_pool_1;
create resource pool test_pool_2 with(mem_percent=11, cpu_affinity=1);
create resource pool TEst_pool_3 with(mem_percent=11);
create resource pool "test_pool_4" with(cpu_affinity=1);
create resource pool test_pool_5 with(control_group="low");
create resource pool test_pool_6 with(control_group="mediuM");
create resource pool test_pool_7 with(control_group="HIGH");
create resource pool test_pool_8 with(control_group="Rush");
select * from pg_resource_pool;

---create resource pool error.
create resource pool test_pool_1 with(mem_percent=11, cpu_affinity=1);
create resource pool test_pool_error_1 with(mem_percent=11, mem_percent=11, cpu_affinity=1);
create resource pool test_pool_error_2 with(mem_percent=11, cpu_affinity=1, cpu_affinity=1);
create resource pool test_pool_error_3 with(memory_percen=12);

create resource pool test_pool_error_4 with(mem_percent=101);
create resource pool test_pool_error_5 with(mem_percent=-1);
create resource pool test_pool_error_6 with(mem_percent=99);

create resource pool test_pool_error_7 with(cpu_affinity=-2);

create resource pool test_pool_error_8 with(cpu_affinity=0);
create resource pool test_pool_error_9 with(cpu_affinity=-1);

create resource pool test_pool_error_10 with(control_group="undefined");

create resource pool test_pool_error_12 with(control_group="rush", cpu_affinity=2);
create resource pool test_pool_error_13 with(cpu_affinity=2, control_group="rush");

select * from pg_resource_pool;

---alter resource pool success.
alter resource pool test_pool_1 with(mem_percent=12, cpu_affinity=2);
alter resource pool test_pool_2 with(mem_percent=31);
alter resource pool test_pool_3 with(cpu_affinity=3);
alter resource pool test_pool_5 with(control_group="high");
select * from pg_resource_pool;

---alter resource pool error.
alter resource pool none_resource_pool with(mem_percent=12, cpu_affinity=2);
alter resource pool test_pool_1;
alter resource pool test_pool_1 with(mem_percent='1a');
alter resource pool test_pool_1 with(cpu_affinity='1a');
alter resource pool test_pool_1 with(control_group="undefined");
alter resource pool test_pool_4 with(control_group="rush");
alter resource pool test_pool_5 with(cpu_affinity=1);
alter resource pool test_pool_1 with(control_group="rush", cpu_affinity=2);
alter resource pool test_pool_1 with(cpu_affinity=2, control_group="rush");
select * from pg_resource_pool;

---drop resource pool success.
drop resource pool test_pool_4;
drop resource pool if exists TEst_pool_3;
drop resource pool if exists test_pool_4;
select * from pg_resource_pool;

---drop resource pool error part 1.
drop resource pool test_pool_4;
select * from pg_resource_pool;

---create workload group success.
create workload group test_group_1 using resource pool test_pool_1 with(act_statements=3);
create workload group test_group_2;
create workload group test_group_3 using resource pool test_pool_1;
create workload group test_group_4 with(act_statements=10);
select * from pg_workload_group;

---create workload group error.
create workload group test_group_1 using resource pool test_pool_1 with(act_statements=3);
create workload group test_group_5 with(act_statements=10, act_statements=12);
create workload group test_group_6 with(active_statem=11);

create workload group test_group_7 using resource pool none_resource_pool;

create workload group test_group_8 with(act_statements=-1);
select * from pg_workload_group;

---alter workload group success.
alter workload group test_group_1 using resource pool default_pool with(act_statements=4);
alter workload group test_group_1 using resource pool test_pool_1;
alter workload group test_group_1 with(act_statements=5);
select * from pg_workload_group;

---alter workload group error.
alter workload group no_group using resource pool default_pool with(act_statements=4);
alter workload group test_group_1;
alter workload group test_group_1 using resource pool default_pool with(act_statements='1a');
select * from pg_workload_group;

---drop workload group success.
drop workload group test_group_3;
drop workload group if exists test_group_3;
drop workload group if exists test_group_4;
select * from pg_workload_group;

---drop resource pool error part 2.
drop resource pool test_pool_1;

---drop workload group error part 1.
drop workload group none_group;

---create application success.
create app workload group mapping test_app_1;
create app workload group mapping test_app_2 with(workload_gpname='test_group_1');
create app workload group mapping test_app_3 with(workload_gpname='default_group');
select * from pg_app_workloadgroup_mapping;

---create application error.
create app workload group mapping test_app_1 with(workload_gpname='test_group_1');
create app workload group mapping test_app_1 with(workload_gpname='none_group');
select * from pg_app_workloadgroup_mapping;

---alter application success.
alter app workload group mapping test_app_1 with(workload_gpname='test_group_1');
alter app workload group mapping test_app_2 with(workload_gpname='default_group');
select * from pg_app_workloadgroup_mapping;

---alter application error.
alter app workload group mapping none_app with(workload_gpname='default_group');
alter app workload group mapping test_app_1 with(workload_gpname=111);
select * from pg_app_workloadgroup_mapping;

---drop application success.
drop app workload group mapping test_app_2;
drop app workload group mapping if exists test_app_2;
drop app workload group mapping if exists test_app_3;
select * from pg_app_workloadgroup_mapping;

---drop workload group error part 2.
drop workload group test_group_1;

---drop application error.
drop app workload group mapping none_app;
select * from pg_app_workloadgroup_mapping;
