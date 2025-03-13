--upgrade TABLE
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9815, 2989, 8633, 8636;
 CREATE TABLE pg_catalog.pg_object_type (                                                      
     typoid oid NOT NULL,                                                           
     supertypeoid oid NOT NULL,                                                     
     isfinal boolean NOT NULL,   
     isinstantiable boolean NOT NULL,   
     ispersistable boolean NOT NULL,           
     isbodydefined boolean NOT NULL,                                                   
     mapmethod oid NOT NULL,                                                        
     ordermethod oid NOT NULL,                                                      
     objectrelid oid NOT NULL, 
     objectoptions int NOT NULL,                                                     
     typespecsrc text,                                                              
     typebodydeclsrc text,
     objectextensions text[]                                                           
 )WITH(oids=true) TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2990;
CREATE UNIQUE INDEX pg_catalog.pg_object_type_index ON pg_object_type USING btree (typoid) TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2992;
CREATE UNIQUE INDEX pg_catalog.pg_object_type_oid_index ON pg_object_type USING btree (oid) TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
GRANT SELECT ON pg_catalog.pg_object_type TO PUBLIC;