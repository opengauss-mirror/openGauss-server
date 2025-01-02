-- wait for server establishment

CREATE OR REPLACE FUNCTION wait_for_gms_debug_extension()
RETURNS BOOLEAN AS $$
DECLARE
    extension_exists BOOLEAN;
BEGIN
    -- 初始化变量
    extension_exists := FALSE;

    -- 循环查询扩展是否存在
    WHILE NOT extension_exists LOOP
        -- 查询扩展是否存在
        PERFORM 1 FROM pg_extension WHERE extname = 'gms_debug';
        IF FOUND THEN
            -- 如果扩展存在，则退出循环
            extension_exists := TRUE;
        ELSE
            -- 如果扩展不存在，则等待一段时间再重试
            PERFORM pg_sleep(1); -- 等待1秒
        END IF;
    END LOOP;

    -- 返回扩展存在的标志
    RETURN extension_exists;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF wait_for_gms_debug_extension() THEN
        -- 扩展存在，执行下一步操作
    END IF;
END $$;

set search_path = gms_debugger_test1;
set behavior_compat_options='proc_outparam_override';

CREATE or REPLACE FUNCTION gms_breakpoint(funcname text, lineno int)
returns void as $$
declare 
    pro_info  gms_debug.program_info;
    bkline     binary_integer;
    ret     binary_integer;
begin
    pro_info.name := funcname;
    ret := gms_debug.set_breakpoint(pro_info, lineno, bkline,1,1);
    RAISE NOTICE 'ret= %', ret;
    RAISE NOTICE 'ret= %', bkline;
end;
$$ LANGUAGE plpgsql;

CREATE or REPLACE FUNCTION gms_continue()
returns void as $$
declare
    run_info  gms_debug.runtime_info;
    ret     binary_integer;
begin
    ret := gms_debug.continue(run_info, gms_debug.break_any_return, 2);
    RAISE NOTICE 'breakpoint= %', run_info.breakpoint;
    RAISE NOTICE 'stackdepth= %', run_info.stackdepth;
    RAISE NOTICE 'line= %', run_info.line#;
    RAISE NOTICE 'reason= %', run_info.reason;
    RAISE NOTICE 'ret= %',ret;
end;
$$ LANGUAGE plpgsql;

CREATE or REPLACE FUNCTION gms_next()
returns void as $$
declare
    run_info  gms_debug.runtime_info;
    ret     binary_integer;
begin
    ret := gms_debug.continue(run_info, gms_debug.break_next_line, 2);
    RAISE NOTICE 'breakpoint= %', run_info.breakpoint;
    RAISE NOTICE 'stackdepth= %', run_info.stackdepth;
    RAISE NOTICE 'line= %', run_info.line#;
    RAISE NOTICE 'reason= %', run_info.reason;
    RAISE NOTICE 'ret= %',ret;
end;
$$ LANGUAGE plpgsql;

CREATE or REPLACE FUNCTION gms_step()
returns void as $$
declare
    run_info  gms_debug.runtime_info;
    ret     binary_integer;
begin
    ret := gms_debug.continue(run_info, gms_debug.break_any_call, 2);
    RAISE NOTICE 'breakpoint= %', run_info.breakpoint;
    RAISE NOTICE 'stackdepth= %', run_info.stackdepth;
    RAISE NOTICE 'line= %', run_info.line#;
    RAISE NOTICE 'reason= %', run_info.reason;
    RAISE NOTICE 'ret= %',ret;
end;
$$ LANGUAGE plpgsql;


CREATE or REPLACE FUNCTION gms_info()
returns void as $$
declare
    run_info  gms_debug.runtime_info;
    ret     binary_integer;
begin
    ret := gms_debug.get_runtime_info(1, run_info);
    RAISE NOTICE 'breakpoint= %', run_info.breakpoint;
    RAISE NOTICE 'stackdepth= %', run_info.stackdepth;
    RAISE NOTICE 'line= %', run_info.line#;
    RAISE NOTICE 'reason= %', run_info.reason;
    RAISE NOTICE 'ret= %',ret;
end;
$$ LANGUAGE plpgsql;

-- attach debug server
select * from gms_debug.attach_session('datanode1-0', 1);

select pg_sleep(3);

select gms_breakpoint('test_debug', 0); -- negative

select gms_breakpoint('test_debug', 15); -- ok

select  gms_breakpoint('test_debug', 17); -- invalid
select  gms_breakpoint('test_debug', 22); -- ok
select  gms_breakpoint('test_debug', 15); -- ok

select gms_next();

select gms_next();

select gms_next();

select gms_next();

select gms_continue();

select gms_continue();

select gms_debug.detach_session();
