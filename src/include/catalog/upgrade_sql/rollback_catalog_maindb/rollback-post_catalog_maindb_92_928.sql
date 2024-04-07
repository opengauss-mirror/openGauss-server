
UPDATE pg_proc SET proname = 'varchar_transform', prosrc='varchar_transform' WHERE oid = 3097;
UPDATE pg_proc SET proname = 'numeric_transform', prosrc='numeric_transform' WHERE oid = 3157;
UPDATE pg_proc SET proname = 'varbit_transform', prosrc='varbit_transform' WHERE oid = 3158;
UPDATE pg_proc SET proname = 'timestamp_transform', prosrc='timestamp_transform' WHERE oid = 3917;
UPDATE pg_proc SET proname = 'interval_transform', prosrc='interval_transform' WHERE oid = 3918;
UPDATE pg_proc SET proname = 'time_transform', prosrc='time_transform' WHERE oid = 3944;
