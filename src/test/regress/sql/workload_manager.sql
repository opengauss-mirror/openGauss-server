create resource pool respool_01 with(active_statements = 2147483647); -- INT_MAX
create resource pool respool_02 with(active_statements = 2147483648);
create resource pool respool_02 with(active_statements = 21474836470);
create resource pool respool_02 with(active_statements = -2);

create resource pool respool_02 with(io_limits = 2147483647);  -- INT_MAX
create resource pool respool_03 with(io_limits = 2147483648);
create resource pool respool_03 with(io_limits = 21474836470);
create resource pool respool_03 with(io_limits = -2);




select * from pg_resource_pool;

drop resource pool respool_01;
drop resource pool respool_02;

