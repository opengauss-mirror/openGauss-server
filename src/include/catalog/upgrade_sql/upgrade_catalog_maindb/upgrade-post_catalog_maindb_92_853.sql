DO $$
BEGIN
    -- there is a historical error, only v3.0.5(92612), v5.0.1(92854) and subsequent patch version of them have this function.
    -- version before v3.0.3(92608) and v5.0.0(92848) need to create function.
    if working_version_num() <= 92608 OR working_version_num() = 92848 then
        set local inplace_upgrade_next_system_object_oids = IUO_PROC, 3148;
        CREATE OR REPLACE FUNCTION pg_catalog.pg_terminate_active_session_socket
        (IN threadid BIGINT,
          IN sessionid BIGINT)
        RETURNS bool LANGUAGE INTERNAL NOT FENCED as 'pg_terminate_active_session_socket';
    end if;
END
$$;