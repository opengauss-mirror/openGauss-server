do $$
DECLARE
ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select extname from pg_extension where extname='dolphin')
    LOOP
        if ans = true then
            if working_version_num() < 92850 then
                ALTER EXTENSION dolphin UPDATE TO '1.0';
            elseif working_version_num() = 92850 then
                ALTER EXTENSION dolphin UPDATE TO '1.1';
            else
                BEGIN
                    ALTER EXTENSION dolphin UPDATE TO '1.9';
                EXCEPTION WHEN invalid_parameter_value THEN
                    BEGIN
                        ALTER EXTENSION dolphin UPDATE TO '1.8';
                    EXCEPTION WHEN invalid_parameter_value THEN
                        BEGIN
                            ALTER EXTENSION dolphin UPDATE TO '1.7';
                        EXCEPTION WHEN invalid_parameter_value THEN
                            BEGIN
                                ALTER EXTENSION dolphin UPDATE TO '1.6';
                            EXCEPTION WHEN invalid_parameter_value THEN
                                BEGIN
                                    ALTER EXTENSION dolphin UPDATE TO '1.5';
                                EXCEPTION WHEN invalid_parameter_value THEN
                                    BEGIN
                                        ALTER EXTENSION dolphin UPDATE TO '1.4';
                                    EXCEPTION WHEN invalid_parameter_value THEN
                                        BEGIN
                                            ALTER EXTENSION dolphin UPDATE TO '1.3';
                                        EXCEPTION WHEN invalid_parameter_value THEN
                                            BEGIN
                                                ALTER EXTENSION dolphin UPDATE TO '1.2';
                                            EXCEPTION WHEN invalid_parameter_value THEN
                                            END;
                                        END;
                                    END;
                                END;
                            END;
                        END;
                    END;
                END;
            end if;
        end if;
        exit;
    END LOOP;
END$$;