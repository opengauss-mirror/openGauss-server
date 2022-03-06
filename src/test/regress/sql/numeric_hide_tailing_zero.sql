set behavior_compat_options='';
select cast(123.123 as numeric(15,10));
set behavior_compat_options='hide_tailing_zero';
select cast(123.123 as numeric(15,10));
select cast(0 as numeric(15,10));
select cast(009.0000 as numeric(15,10));
set behavior_compat_options='';