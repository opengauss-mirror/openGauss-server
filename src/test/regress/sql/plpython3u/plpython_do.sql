DO $$ plpy.notice("This is plpythonu.") $$ LANGUAGE plpython3u;

DO $$ plpy.notice("This is plpython2u.") $$ LANGUAGE plpython3u;

DO $$ raise Exception("error test") $$ LANGUAGE plpython3u;
