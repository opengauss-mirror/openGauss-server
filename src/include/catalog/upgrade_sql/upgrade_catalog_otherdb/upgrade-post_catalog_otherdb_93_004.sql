DROP FUNCTION IF EXISTS pg_catalog.appendchildxml(xml, text, text, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8800;                                                                  
CREATE OR REPLACE FUNCTION pg_catalog.appendchildxml (xml, text, text, text[]) RETURNS xml LANGUAGE INTERNAL IMMUTABLE  as 'xmltype_appendchildxml';

DROP FUNCTION IF EXISTS pg_catalog.appendchildxml(xml, text, xml, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8801;            
CREATE OR REPLACE FUNCTION pg_catalog.appendchildxml (xml, text, xml, text[]) RETURNS xml LANGUAGE INTERNAL IMMUTABLE  as 'xmltype_appendchildxml';

DROP FUNCTION IF EXISTS pg_catalog.appendchildxml(xml, text, xml) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8802;
CREATE OR REPLACE FUNCTION pg_catalog.appendchildxml (xml, text, xml) RETURNS xml LANGUAGE INTERNAL IMMUTABLE  as 'select pg_catalog.appendchildxml($1, $2, $3, ''{}''::pg_catalog.text[])'; 

DROP FUNCTION IF EXISTS pg_catalog.appendchildxml(xml, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8803;
CREATE OR REPLACE FUNCTION pg_catalog.appendchildxml (xml, text, text) RETURNS xml LANGUAGE INTERNAL IMMUTABLE  as 'select pg_catalog.appendchildxml($1, $2, $3, ''{}''::pg_catalog.text[])';

DROP FUNCTION IF EXISTS pg_catalog.existsnode (xml, character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8052;
CREATE OR REPLACE FUNCTION pg_catalog.existsnode (xml, character varying) RETURNS tinyint LANGUAGE INTERNAL IMMUTABLE STRICT as 'xmltype_existsnode';

DROP FUNCTION IF EXISTS pg_catalog.extract_internal (xml, character varying, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8580;
CREATE OR REPLACE FUNCTION pg_catalog.extract_internal (xml, character varying, text[]) RETURNS xml LANGUAGE INTERNAL IMMUTABLE  as 'xmltype_extract';
comment on function pg_catalog.extract_internal (xml, character varying, text[]) is 'evaluate XPath expression, with namespace support';

DROP FUNCTION IF EXISTS pg_catalog.extract_internal (xml, character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8581;
CREATE OR REPLACE FUNCTION pg_catalog.extract_internal (xml, character varying) RETURNS xml LANGUAGE INTERNAL IMMUTABLE  as 'select pg_catalog.extract_internal($1, $2, ''{}''::pg_catalog.text[])';
comment on function pg_catalog.extract_internal (xml, character varying) is 'evaluate XPath expression, without namespace support';

DROP FUNCTION IF EXISTS pg_catalog.extractvalue (xml, character varying, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8051;
CREATE OR REPLACE FUNCTION pg_catalog.extractvalue (xml, character varying, text[]) RETURNS character varying LANGUAGE INTERNAL IMMUTABLE STRICT as 'xmltype_extractvalue';

DROP FUNCTION IF EXISTS pg_catalog.extractvalue (xml, character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8053;
CREATE OR REPLACE FUNCTION pg_catalog.extractvalue (xml, character varying) RETURNS character varying LANGUAGE INTERNAL IMMUTABLE STRICT as 'select pg_catalog.extractvalue($1, $2, ''{}''::pg_catalog.text[])';

DROP FUNCTION IF EXISTS pg_catalog.getstringval (xml) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8583;
CREATE OR REPLACE FUNCTION pg_catalog.getstringval (xml) RETURNS character varying LANGUAGE INTERNAL IMMUTABLE STRICT as 'xmltype_getstringval';

DROP FUNCTION IF EXISTS pg_catalog.getstringval (xml[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8584;
CREATE OR REPLACE FUNCTION pg_catalog.getstringval (xml[]) RETURNS character varying LANGUAGE INTERNAL IMMUTABLE STRICT as 'xmltype_getstringval_array';

DROP FUNCTION IF EXISTS pg_catalog.xmlsequence (xml) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8586;
CREATE OR REPLACE FUNCTION pg_catalog.xmlsequence (xml) RETURNS xml[] LANGUAGE INTERNAL IMMUTABLE  as 'xmltype_xmlsequence';

DROP FUNCTION IF EXISTS pg_catalog.xmlsequence (xml[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8587;
CREATE OR REPLACE FUNCTION pg_catalog.xmlsequence (xml[]) RETURNS xml[] LANGUAGE INTERNAL IMMUTABLE STRICT as 'xmltype_xmlsequence_array';
