drop schema if exists function_default cascade;
create schema function_default;
set current_schema = function_default;
show behavior_compat_options;

-- 创建存储过程
create or replace procedure pro_default(p1 text,p2 int default 123,p3 int)
as
begin
raise info 'p1:%',p1;
raise info 'p2:%',p2;
raise info 'p3:%',p3;
end;
/
-- 调用存储过程，预期报错
call pro_default('test',1);

-- 开启逃生参数，保持原本表现
set behavior_compat_options="proc_uncheck_default_param";
show behavior_compat_options;
call pro_default('test',1);

-- 清理环境
drop schema function_default cascade;
reset behavior_compat_options;
