-- calendaring syntax check --
create or replace procedure eval16(calendar_str text) as
declare
    start_date        timestamp with time zone;
    return_date_after timestamp with time zone;
    next_run_date     timestamp with time zone;
begin
    start_date := '2003-2-1 10:30:00.111111+8'::timestamp with time zone;
    return_date_after := start_date;
    -- print 16 consecutive next dates --
    FOR i in 1..16 loop
        DBE_SCHEDULER.EVALUATE_CALENDAR_STRING(
            calendar_str,
            start_date, return_date_after, next_run_date);
        DBE_OUTPUT.PRINT_LINE('next_run_date: ' || next_run_date);
        return_date_after := next_run_date;
    end loop;
end;
/

show timezone;

-- problems: ORA does not support these --
call eval16('FREQ=weekly;INTERVAL=50;BYMONTH=2,3;BYHOUR=10;BYMINUTE=20,30,40;BYSECOND=0');
call eval16('FREQ=secondly;BYMONTH=6;'); -- hard to find, but worked

-- problem: ORA generate different result --
call eval16('FREQ=weekly;INTERVAL=40;BYMONTH=2,3;BYHOUR=10;BYMINUTE=20,30,40;BYSECOND=0');

-- compiled scene --
call eval16('FREQ=hourly;INTERVAL=2;BYHOUR=6,10;BYMINUTE=0;BYSECOND=0'); -- good
call eval16('FREQ=hourly;INTERVAL=2;BYHOUR=6,9;BYMINUTE=0;BYSECOND=0'); -- good, only 6 o'clock
call eval16('FREQ=weekly;INTERVAL=3;BYMONTH=2,3;BYHOUR=10;BYMINUTE=20,30,40;BYSECOND=0');
call eval16('FREQ=yearly;INTERVAL=50;BYMONTH=2,3,4,5,6,7,8,9,11,12;BYHOUR=10;BYMINUTE=1,2,3,4,5,6,7,8,9,20,30,40;BYSECOND=0'); -- fine performance
call eval16('FREQ=secondly;INTERVAL=50;BYMONTH=2,3,4,5,6,7,8,9,11,12;BYHOUR=10;BYMINUTE=1,2,3,4,5,6,7,8,9,20,30,40;BYSECOND=0'); -- fixed, large loops
call eval16('FREQ=secondly;INTERVAL=50;BYMONTH=2,3,4,5,6,7,8,9,11,12;BYMONTHDAY=1,3,5,7,9;BYHOUR=1,3,10,13,15,17;BYMINUTE=1,2,3,4,5,6,7,8,9,20,30,40;BYSECOND=0'); -- a looooot of params
call eval16('FREQ=secondly;INTERVAL=50;BYMONTH=2,3,4,5,6,7,8,9,11,12;BYMONTHDAY=1,3,5,7,9;BYHOUR=1,3,5,10,13,15,17;BYMINUTE=20,30,40,1,2,3,4,5,6,7,8,9;BYSECOND=0'); -- still good
call eval16('FREQ=secondly;INTERVAL=59;BYMONTH=2,3;BYHOUR=10;BYMINUTE=20,30,40;BYSECOND=58'); -- secondly works fine
call eval16('FREQ=minutely;INTERVAL=50;BYMONTH=1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1;BYMONTHDAY=-1;BYHOUR=1;BYMINUTE=0;BYSECOND=0');

-- error scenes --
call eval16('FREQ=secondly;INTERVAL=50;BYMONTH=6;BYMONTHDAY=6;BYHOUR=10;BYMINUTE=0;BYSECOND=1'); -- not reachable
call eval16('FREQ=secondly;BYMONTH=6;BYNOTHING=6;');
