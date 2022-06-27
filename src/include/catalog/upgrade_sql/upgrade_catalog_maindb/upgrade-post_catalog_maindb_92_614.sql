INSERT INTO pg_catalog.pg_attribute(attrelid,attname,atttypid,attstattarget,attlen,attnum,attndims,attcacheoff,atttypmod,
attbyval,attstorage,attalign,attnotnull,atthasdef,attisdropped,attislocal,attcmprmode,attinhcount,attcollation,attkvtype)
VALUES (2604,'adbin_on_update',194,-1,-1,6,0,-1,-1,false,'x','i',false,false,false,true,0,0,100,0),
(2604,'adsrc_on_update',25,-1,-1,7,0,-1,-1,false,'x','i',false,false,false,true,0,0,100,0);

UPDATE PG_CATALOG.pg_class set relnatts = 7 where relname = 'pg_attrdef' AND relnamespace = 11;

UPDATE PG_CATALOG.pg_attrdef SET adsrc_on_update='';
