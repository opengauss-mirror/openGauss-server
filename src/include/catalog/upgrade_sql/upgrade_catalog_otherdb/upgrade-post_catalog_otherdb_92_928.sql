
UPDATE pg_proc SET proname = 'varchar_support', prosrc='varchar_support' WHERE oid = 3097;
UPDATE pg_proc SET proname = 'numeric_support', prosrc='numeric_support' WHERE oid = 3157;
UPDATE pg_proc SET proname = 'varbit_support', prosrc='varbit_support' WHERE oid = 3158;
UPDATE pg_proc SET proname = 'timestamp_support', prosrc='timestamp_support' WHERE oid = 3917;
UPDATE pg_proc SET proname = 'interval_support', prosrc='interval_support' WHERE oid = 3918;
UPDATE pg_proc SET proname = 'time_support', prosrc='time_support' WHERE oid = 3944;
