DO $$
BEGIN
    -- 92780 is openGauss 3.1.0, which has dolphin, so should not drop it.
    if working_version_num() < 92780 then
        drop extension if exists dolphin;
    end if;
END
$$;
