DO $$
DECLARE
  dbcom text;
BEGIN
  show sql_compatibility into dbcom;
  if dbcom = 'A' THEN
      EXECUTE
        'CREATE OR REPLACE TYPE pg_catalog.ANYTYPE as object(
          prec int,
          scale int,
          len int,
          csid int,
          csfrm int,
          typecode int,
          schema_name name,
          type_name name,
          version varchar2,
          numelems int,
          elem_tc int,
          elem_count int,
          state int,

          STATIC PROCEDURE BEGINCREATE(typecode IN int, atype OUT pg_catalog.ANYTYPE),
          MEMBER PROCEDURE SETINFO(
              self          IN OUT pg_catalog.ANYTYPE,
              prec          IN int,
              scale         IN int,
              len           IN bigint,
              csid          IN int,
              csfrm         IN int,
              atype         IN pg_catalog.ANYTYPE DEFAULT NULL,
              elem_tc       IN int DEFAULT NULL,
              elem_count    IN int DEFAULT 0
          ),
          MEMBER PROCEDURE ENDCREATE(self IN OUT pg_catalog.ANYTYPE),
          MEMBER FUNCTION GETINFO(
              self        IN pg_catalog.ANYTYPE,
              prec        OUT int, 
              scale       OUT int,
              len         OUT int, 
              csid        OUT int,
              csfrm       OUT int,
              schema_name OUT name, 
              type_name   OUT name, 
              version     OUT varchar2,
              numelems    OUT int
          ) RETURN int
      );

      GRANT USAGE ON TYPE pg_catalog.ANYTYPE TO PUBLIC;

      CREATE OR REPLACE TYPE BODY pg_catalog.ANYTYPE AS
          STATIC PROCEDURE BEGINCREATE(typecode IN int, atype OUT pg_catalog.ANYTYPE) AS
          begin
              IF NOT (pg_typeof(atype) = ''anytype''::regtype) AND (atype IS NULL) THEN
                  raise exception ''expression "NULL" cannot be used as an assignment target'';
              ELSIF typecode IN (101, 113, 96, 12, 286, 2, 287, 95, 187, 188, 1, 9) THEN
                  atype.typecode = typecode;
              ELSE
                  raise exception ''invalid typecode'';
              END IF; 
              atype.state := 0;
          end;

          MEMBER PROCEDURE SETINFO(
              self          IN OUT pg_catalog.ANYTYPE,
              prec          IN int,
              scale         IN int,
              len           IN bigint,
              csid          IN int,
              csfrm         IN int,
              atype         IN pg_catalog.ANYTYPE DEFAULT NULL,
              elem_tc       IN int DEFAULT NULL,
              elem_count    IN int DEFAULT 0
          ) AS
          begin
              IF self IS NULL THEN
                  raise exception ''expression "NULL" cannot be used as an assignment target'';
              ELSIF self.state = 2 THEN
            raise exception ''incorrect usage of method SETINFO'';
              ELSIF (prec < 0 OR prec > 255 OR scale < -128 OR scale >127 OR len < 0 OR len > 2147483647 OR csid < 0 OR csid > 65535 OR csfrm < 0 OR csfrm > 255) THEN
                  raise exception ''numeric overflow'';
              ELSE
                  CASE
                      WHEN self.typecode IN (1, 9, 96) THEN
                          self.len := len;
                          self.csid := csid;
                          self.csfrm := csfrm % 32;
                      WHEN self.typecode = 2 THEN
                          self.prec = prec;
                          self.scale := scale;
                      WHEN self.typecode = 95 THEN
                          self.len := len;
                      ELSE
                  END CASE;
                  self.state = 1;
              END IF; 
          end;

          MEMBER PROCEDURE ENDCREATE(self IN OUT pg_catalog.ANYTYPE) AS
          begin
              IF self IS NULL THEN
                  raise exception ''expression "NULL" cannot be used as an assignment target'';
              ELSIF self.state != 1 THEN
                raise exception ''incorrect usage of method ENDCREATE'';
              ELSE
                self.state := 2;
              END IF; 
          end;

          MEMBER FUNCTION GETINFO(
              self        IN pg_catalog.ANYTYPE,
              prec        OUT int, 
              scale       OUT int,
              len         OUT int, 
              csid        OUT int,
              csfrm       OUT int,
              schema_name OUT name, 
              type_name   OUT name, 
              version     OUT varchar2,
              numelems    OUT int
          ) RETURN int AS
          declare
              typecode int;
          begin
              IF self IS NULL THEN
                  raise exception ''expression "NULL" cannot be used as an assignment target'';
              ELSIF self.state != 2 THEN
                  raise exception ''incorrect usage of method GETINFO'';
              ELSE
                  prec := self.prec;
                  scale := self.scale;
                  len := self.len;
                  csid := self.csid;
                  csfrm := self.csfrm;
                  schema_name := self.schema_name;
                  type_name := self.type_name;
                  version := self.version;
                  numelems := self.numelems;
                  typecode := self.typecode;
              END IF;
              return typecode;
          end;
        END;';
  end if;
END $$;

DO $$
DECLARE
  dbcom text;
BEGIN
  show sql_compatibility into dbcom;
  if dbcom = 'A' THEN
      EXECUTE
      'CREATE OR REPLACE TYPE pg_catalog.ANYDATA as object(
          data text,
          type_info pg_catalog.ANYTYPE,
          type_name name,
          typecode int,

          STATIC FUNCTION ConvertBDouble(dbl IN BINARY_DOUBLE) return pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertBlob(b IN BLOB) RETURN pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertChar(c IN CHAR) RETURN pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertDate(dat IN DATE) RETURN pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertNchar(nc IN NCHAR) return pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertNVarchar2(nc IN NVARCHAR2) return pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertNumber(num IN NUMBER) RETURN pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertRaw(r IN RAW) RETURN pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertTimestamp(ts IN TIMESTAMP) return pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertTimestampTZ(ts IN TIMESTAMP WITH TIME ZONE) return pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertVarchar(c IN VARCHAR) RETURN pg_catalog.ANYDATA,
          STATIC FUNCTION ConvertVarchar2(c IN VARCHAR2) RETURN pg_catalog.ANYDATA,

          MEMBER FUNCTION AccessBDouble(self IN pg_catalog.ANYDATA) return BINARY_DOUBLE,
          MEMBER FUNCTION AccessBlob(self IN pg_catalog.ANYDATA) return BLOB,
          MEMBER FUNCTION AccessChar(self IN pg_catalog.ANYDATA) return CHAR,
          MEMBER FUNCTION AccessDate(self IN pg_catalog.ANYDATA) return DATE,
          MEMBER FUNCTION AccessNchar(self IN pg_catalog.ANYDATA) return NCHAR,
          MEMBER FUNCTION AccessNumber(self IN pg_catalog.ANYDATA) return NUMBER,
          MEMBER FUNCTION AccessNVarchar2(self IN pg_catalog.ANYDATA) return NVARCHAR2,
          MEMBER FUNCTION AccessRaw(self IN pg_catalog.ANYDATA) return RAW,
          MEMBER FUNCTION AccessTimestamp(self IN pg_catalog.ANYDATA) return TIMESTAMP,
          MEMBER FUNCTION AccessTimestampTZ(self IN pg_catalog.ANYDATA) return TIMESTAMP WITH TIME ZONE,
          MEMBER FUNCTION AccessVarchar(self IN pg_catalog.ANYDATA) return VARCHAR,
          MEMBER FUNCTION AccessVarchar2(self IN pg_catalog.ANYDATA) return VARCHAR2,

          MEMBER FUNCTION GETTYPE(self IN pg_catalog.ANYDATA, typ OUT pg_catalog.ANYTYPE) RETURN INT,
          MEMBER FUNCTION GETTYPENAME(self IN pg_catalog.ANYDATA) RETURN VARCHAR2
      );


      GRANT USAGE ON TYPE pg_catalog.ANYDATA TO PUBLIC;

      CREATE OR REPLACE TYPE BODY pg_catalog.ANYDATA AS
          STATIC FUNCTION ConvertBDouble(dbl IN BINARY_DOUBLE) return pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := dbl::text;
              v_anydata.type_name := ''BINARY_DOUBLE'';
              v_anydata.typecode := 101;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessBDouble(self IN pg_catalog.ANYDATA) return BINARY_DOUBLE AS
          begin
              IF self.type_name = ''BINARY_DOUBLE'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertBlob(b IN BLOB) RETURN pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := b::raw::text;
              v_anydata.type_name := ''Blob'';
              v_anydata.typecode := 113;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessBlob(self IN pg_catalog.ANYDATA) return BLOB AS
          begin
              IF self.type_name = ''Blob'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertChar(c IN CHAR) RETURN pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := c::text;
              v_anydata.type_name := ''Char'';
              v_anydata.typecode := 96;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessChar(self IN pg_catalog.ANYDATA) return CHAR AS
          begin
              IF self.type_name = ''Char'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertDate(dat IN DATE) RETURN pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := dat::text;
              v_anydata.type_name := ''Date'';
              v_anydata.typecode := 12;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessDate(self IN pg_catalog.ANYDATA) return DATE AS
          begin
              IF self.type_name = ''Date'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertNchar(nc IN NCHAR) return pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := nc::text;
              v_anydata.type_name := ''NChar'';
              v_anydata.typecode := 286;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessNchar(self IN pg_catalog.ANYDATA) return NCHAR AS
          begin
              IF self.type_name = ''NChar'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertNVarchar2(nc IN NVARCHAR2) return pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := nc::text;
              v_anydata.type_name := ''NVarchar2'';
              v_anydata.typecode := 287;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessNVarchar2(self IN pg_catalog.ANYDATA) return NVARCHAR2 AS
          begin
              IF self.type_name = ''NVarchar2'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertNumber(num IN NUMBER) RETURN pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := num::text;
              v_anydata.type_name := ''Number'';
              v_anydata.typecode := 2;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessNumber(self IN pg_catalog.ANYDATA) return NUMBER AS
          begin
              IF self.type_name = ''Number'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertRaw(r IN RAW) RETURN pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := r::text;
              v_anydata.type_name := ''Raw'';
              v_anydata.typecode := 95;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessRaw(self IN pg_catalog.ANYDATA) return RAW AS
          begin
              IF self.type_name = ''Raw'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertTimestamp(ts IN TIMESTAMP) return pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := ts::text;
              v_anydata.type_name := ''Timestamp'';
              v_anydata.typecode := 187;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessTimestamp(self IN pg_catalog.ANYDATA) return TIMESTAMP AS
          begin
              IF self.type_name = ''Timestamp'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertTimestampTZ(ts IN TIMESTAMP WITH TIME ZONE) return pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := ts::text;
              v_anydata.type_name := ''TimestampTZ'';
              v_anydata.typecode := 188;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessTimestampTZ(self IN pg_catalog.ANYDATA) return TIMESTAMP WITH TIME ZONE AS
          begin
              IF self.type_name = ''TimestampTZ'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertVarchar(c IN VARCHAR) RETURN pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := c::text;
              v_anydata.type_name := ''Varchar'';
              v_anydata.typecode := 1;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessVarchar(self IN pg_catalog.ANYDATA) return VARCHAR AS
          begin
              IF self.type_name = ''Varchar'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          STATIC FUNCTION ConvertVarchar2(c IN VARCHAR2) RETURN pg_catalog.ANYDATA AS
          declare
              v_anydata pg_catalog.ANYDATA;
          begin
              v_anydata.data := c::text;
              v_anydata.type_name := ''Varchar2'';
              v_anydata.typecode := 9;
              return v_anydata;
          end;

          MEMBER FUNCTION AccessVarchar2(self IN pg_catalog.ANYDATA) return VARCHAR2 AS
          begin
              IF self.type_name = ''Varchar2'' THEN
                  return textout(self.data);
              ELSE
                  return NULL;
              END IF;
          end;

          MEMBER FUNCTION GETTYPE(self IN pg_catalog.ANYDATA, typ OUT pg_catalog.ANYTYPE) RETURN INT AS
          begin
              return self.typecode;
          end;

          MEMBER FUNCTION GETTYPENAME(self IN pg_catalog.ANYDATA) RETURN VARCHAR2 AS
          begin
              return self.type_name;
          end;
        END;';
  end if;
END $$;


DO $$
DECLARE
  dbcom text;
BEGIN
  show sql_compatibility into dbcom;
  if dbcom = 'A' THEN
      EXECUTE
      'CREATE OR REPLACE TYPE pg_catalog.ANYDATASET as object(
          data text[],
          type_info pg_catalog.ANYTYPE,
          type_name name,
          typecode int,
          count int,
          state bool,

          STATIC PROCEDURE BeginCreate(
            typecode     IN int,
            rtype        IN pg_catalog.AnyType,
            aset         OUT pg_catalog.ANYDATASET),

          MEMBER PROCEDURE AddInstance(self IN OUT pg_catalog.ANYDATASET),

          MEMBER PROCEDURE ENDCREATE(self IN OUT pg_catalog.ANYDATASET),

          MEMBER PROCEDURE SETBDOUBLE(
            self              IN OUT pg_catalog.ANYDATASET, 
            dbl               IN BINARY_DOUBLE, 
            last_elem         IN BOOLEAN DEFAULT FALSE),

          MEMBER PROCEDURE SETBLOB(
            self              IN OUT pg_catalog.ANYDATASET,
            b                 IN BLOB,
            last_elem BOOLEAN DEFAULT FALSE),
          
          MEMBER PROCEDURE SETCHAR(
            self              IN OUT pg_catalog.ANYDATASET,
            c                 IN CHAR,
            last_elem BOOLEAN DEFAULT FALSE),
          
          MEMBER PROCEDURE SETDATE(
            self              IN OUT pg_catalog.ANYDATASET,
            dat               IN DATE,
            last_elem BOOLEAN DEFAULT FALSE),
          
          MEMBER PROCEDURE SETNCHAR(
            self              IN OUT pg_catalog.ANYDATASET,
            nc                IN NCHAR, 
            last_elem IN BOOLEAN DEFAULT FALSE),
          
          MEMBER PROCEDURE SETNUMBER(
            self              IN OUT pg_catalog.ANYDATASET,
            num               IN NUMBER,
            last_elem BOOLEAN DEFAULT FALSE),
          
          MEMBER PROCEDURE SETNVARCHAR2(
            self             IN OUT pg_catalog.ANYDATASET,
            nc               IN NVarchar2, 
            last_elem        IN BOOLEAN DEFAULT FALSE),
          
          MEMBER PROCEDURE SETRAW(
            self              IN OUT pg_catalog.ANYDATASET,
            r                 IN RAW,
            last_elem BOOLEAN DEFAULT FALSE),
          
          MEMBER PROCEDURE SETTIMESTAMP(
            self              IN OUT pg_catalog.ANYDATASET, 
            ts                IN TIMESTAMP,
            last_elem IN BOOLEAN DEFAULT FALSE),
          
          MEMBER PROCEDURE SETTIMESTAMPTZ(
            self             IN OUT pg_catalog.ANYDATASET, 
            ts               IN TIMESTAMP WITH TIME ZONE,
            last_elem        IN BOOLEAN DEFAULT FALSE),
          
          MEMBER PROCEDURE SETVARCHAR(
            self              IN OUT pg_catalog.ANYDATASET,
            c                 IN VARCHAR,
            last_elem BOOLEAN DEFAULT FALSE),
          
          MEMBER PROCEDURE SETVARCHAR2(
            self              IN OUT pg_catalog.ANYDATASET,
            c                 IN VARCHAR2,
            last_elem BOOLEAN DEFAULT FALSE),

          MEMBER FUNCTION GETBDOUBLE(
            self        IN pg_catalog.ANYDATASET, 
            dbl         OUT BINARY_DOUBLE,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETBLOB(
            self        IN pg_catalog.ANYDATASET,
            b           OUT BLOB,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETCHAR(
            self        IN pg_catalog.ANYDATASET,
            c           OUT CHAR,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETDATE(
            self        IN pg_catalog.ANYDATASET,
            dat         OUT DATE,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETNCHAR(
            self        IN pg_catalog.ANYDATASET, 
            nc          OUT NCHAR,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETNUMBER(
            self        IN pg_catalog.ANYDATASET,
            num         OUT NUMBER,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETNVARCHAR2(
            self        IN pg_catalog.ANYDATASET, 
            nc          OUT NVARCHAR2,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETRAW(
            self        IN pg_catalog.ANYDATASET,
            r           OUT RAW,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETTIMESTAMP(
            self        IN pg_catalog.ANYDATASET, 
            ts          OUT TIMESTAMP,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETTIMESTAMPTZ(
            self        IN pg_catalog.ANYDATASET, 
            ts          OUT TIMESTAMP WITH TIME ZONE, 
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETVARCHAR(
            self        IN pg_catalog.ANYDATASET,
            c           OUT VARCHAR,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GETVARCHAR2(
            self        IN pg_catalog.ANYDATASET,
            c           OUT VARCHAR2,
            index       IN int)
          RETURN int,

          MEMBER FUNCTION GetCount(self IN pg_catalog.ANYDATASET) RETURN INT,

          MEMBER FUNCTION GETTYPE(self IN pg_catalog.ANYDATASET, typ OUT pg_catalog.AnyType) RETURN INT,

          MEMBER FUNCTION GETTYPENAME(self IN pg_catalog.ANYDATASET) RETURN VARCHAR2
      );

      GRANT USAGE ON TYPE pg_catalog.ANYDATASET TO PUBLIC;

      CREATE OR REPLACE PROCEDURE
        pg_catalog.setAnydatasetExcept(v_anydataset IN OUT pg_catalog.ANYDATASET, typecode int) AS
        BEGIN
          IF (v_anydataset IS NULL) THEN
            RAISE EXCEPTION ''expression "NULL" cannot be used as an assignment target'';
          ELSIF (v_anydataset.state = 1) OR ((array_length(v_anydataset.data, 1) = 0) AND (v_anydataset.count = 0)) THEN
            RAISE EXCEPTION ''incorrect usage of method SET'';
          ELSIF (v_anydataset.typecode != typecode) THEN
            RAISE EXCEPTION ''Type Mismatch while constructing or accessing OCIAnyData'';
          END IF;
        END;

      CREATE OR REPLACE PROCEDURE
        pg_catalog.getAnydatasetExcept(
            v_anydataset IN pg_catalog.ANYDATASET, 
            index IN int, 
            typecode IN int
        ) AS 
        BEGIN
          IF (v_anydataset IS NULL) THEN
            RAISE EXCEPTION ''expression "NULL" cannot be used as an assignment target'';
          ELSIF (index > v_anydataset.count) OR (index < 1) THEN
            RAISE EXCEPTION ''index out of range'';
          ELSIF (v_anydataset.state = 0) THEN
            RAISE EXCEPTION ''incorrect usage of method GET'';
          ELSIF (v_anydataset.typecode != typecode) THEN
            RAISE EXCEPTION ''Type Mismatch while constructing or accessing OCIAnyData'';
          END IF;
        END;

      CREATE OR REPLACE TYPE BODY pg_catalog.ANYDATASET AS
          STATIC PROCEDURE BeginCreate(
            typecode     IN int,
            rtype        IN pg_catalog.AnyType,
            aset         OUT pg_catalog.ANYDATASET) AS
          begin
            IF NOT (pg_typeof(aset) = ''anydataset''::regtype) AND (aset IS NULL) THEN
              raise exception ''expression "NULL" cannot be used as an assignment target'';
            ELSIF typecode IN (101, 113, 96, 12, 286, 2, 287, 95, 187, 188, 1, 9) THEN
              aset.typecode := typecode;
              aset.data := ARRAY[]::text[];
              aset.count := 0;
              aset.state := 0;
              aset.type_name := CASE typecode
                WHEN 101 THEN ''BDouble''
                WHEN 113 THEN ''Blob''
                WHEN 96  THEN ''Char''
                WHEN 12  THEN ''Date''
                WHEN 286 THEN ''NChar''
                WHEN 2   THEN ''Number''
                WHEN 287 THEN ''NVarchar2''
                WHEN 95  THEN ''Raw''
                WHEN 187 THEN ''Timestamp''
                WHEN 188 THEN ''TimestampTZ''
                WHEN 1   THEN ''Varchar''
                WHEN 9   THEN ''Varchar2''
              END;
            ELSE
              raise exception ''invalid typecode'';
            END IF;
          end;

          MEMBER PROCEDURE AddInstance(self IN OUT pg_catalog.ANYDATASET) AS
          begin
              IF (self IS NULL) THEN
                raise exception ''expression "NULL" cannot be used as an assignment target'';
              ELSIF (self.state != 0) THEN
                raise exception ''incorrect usage of method AddInstance'';
              ELSIF (array_length(self.data, 1) != self.count) THEN
                raise exception ''The Anydataset contains elements that have not been set.'';
              ELSE
                self.count := self.count + 1;
              END IF;
          end;

          MEMBER PROCEDURE ENDCREATE(self IN OUT pg_catalog.ANYDATASET) AS
          begin
            IF (self IS NULL) THEN
              raise exception ''expression "NULL" cannot be used as an assignment target'';
            ELSIF (array_length(self.data, 1) != self.count) THEN
              raise exception ''The Anydataset contains elements that have not been set.'';
            ELSE
              self.state = 1;
            END IF;
          end;

          MEMBER PROCEDURE SETBDOUBLE(
            self              IN OUT pg_catalog.ANYDATASET, 
            dbl               IN BINARY_DOUBLE, 
            last_elem         IN BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 101);
            self.data := array_append(self.data, dbl::text);
          end;

          MEMBER FUNCTION GETBDOUBLE(
            self        IN pg_catalog.ANYDATASET, 
            dbl         OUT BINARY_DOUBLE,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 101);
            dbl = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETBLOB(
            self              IN OUT pg_catalog.ANYDATASET,
            b                 IN BLOB,
            last_elem BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 113);
            self.data := array_append(self.data, b::raw::text);
          end;

          MEMBER FUNCTION GETBLOB(
            self        IN pg_catalog.ANYDATASET,
            b           OUT BLOB,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 113);
            b = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETCHAR(
            self              IN OUT pg_catalog.ANYDATASET,
            c                 IN CHAR,
            last_elem BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 96);
            self.data := array_append(self.data, c::text);
          end;

          MEMBER FUNCTION GETCHAR(
            self        IN pg_catalog.ANYDATASET,
            c           OUT CHAR,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 96);
            c = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETDATE(
            self              IN OUT pg_catalog.ANYDATASET,
            dat               IN DATE,
            last_elem BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 12);
            self.data := array_append(self.data, dat::text);
          end;

          MEMBER FUNCTION GETDATE(
            self        IN pg_catalog.ANYDATASET,
            dat         OUT DATE,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 12);
            dat = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETNCHAR(
            self              IN OUT pg_catalog.ANYDATASET,
            nc                IN NCHAR, 
            last_elem IN BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 286);
            self.data := array_append(self.data, nc::text);
          end;

          MEMBER FUNCTION GETNCHAR(
            self        IN pg_catalog.ANYDATASET, 
            nc          OUT NCHAR,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 286);
            nc = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETNUMBER(
            self              IN OUT pg_catalog.ANYDATASET,
            num               IN NUMBER,
            last_elem BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 2);
            self.data := array_append(self.data, num::text);
          end;

          MEMBER FUNCTION GETNUMBER(
            self        IN pg_catalog.ANYDATASET,
            num         OUT NUMBER,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 2);
            num = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETNVARCHAR2(
            self             IN OUT pg_catalog.ANYDATASET,
            nc               IN NVarchar2, 
            last_elem        IN BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 287);
            self.data := array_append(self.data, nc::text);
          end;

          MEMBER FUNCTION GETNVARCHAR2(
            self        IN pg_catalog.ANYDATASET, 
            nc          OUT NVARCHAR2,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 287);
            nc = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETRAW(
            self              IN OUT pg_catalog.ANYDATASET,
            r                 IN RAW,
            last_elem BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 95);
            self.data := array_append(self.data, r::text);
          end;

          MEMBER FUNCTION GETRAW(
            self        IN pg_catalog.ANYDATASET,
            r           OUT RAW,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 95);
            r = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETTIMESTAMP(
            self              IN OUT pg_catalog.ANYDATASET, 
            ts                IN TIMESTAMP,
            last_elem IN BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 187);
            self.data := array_append(self.data, ts::text);
          end;

          MEMBER FUNCTION GETTIMESTAMP(
            self        IN pg_catalog.ANYDATASET, 
            ts          OUT TIMESTAMP,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 187);
            ts = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETTIMESTAMPTZ(
            self             IN OUT pg_catalog.ANYDATASET, 
            ts               IN TIMESTAMP WITH TIME ZONE,
            last_elem        IN BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 188);
            self.data := array_append(self.data, ts::text);
          end;

          MEMBER FUNCTION GETTIMESTAMPTZ(
            self        IN pg_catalog.ANYDATASET, 
            ts          OUT TIMESTAMP WITH TIME ZONE, 
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 188);
            ts = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETVARCHAR(
            self              IN OUT pg_catalog.ANYDATASET,
            c                 IN VARCHAR,
            last_elem BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 1);
            self.data := array_append(self.data, c::text);
          end;

          MEMBER FUNCTION GETVARCHAR(
            self        IN pg_catalog.ANYDATASET,
            c           OUT VARCHAR,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 1);
            c = textout(self.data[index]);
            return 0;
          end;

          MEMBER PROCEDURE SETVARCHAR2(
            self              IN OUT pg_catalog.ANYDATASET,
            c                 IN VARCHAR2,
            last_elem BOOLEAN DEFAULT FALSE) AS
          begin
            pg_catalog.setAnydatasetExcept(self, 9);
            self.data := array_append(self.data, c::text);
          end;

          MEMBER FUNCTION GETVARCHAR2(
            self        IN pg_catalog.ANYDATASET,
            c           OUT VARCHAR2,
            index       IN int)
          RETURN int AS
          begin
            pg_catalog.getAnydatasetExcept(self, index, 9);
            c = textout(self.data[index]);
            return 0;
          end;

          MEMBER FUNCTION GetCount(self IN pg_catalog.ANYDATASET) RETURN INT AS
          begin
            IF (self IS NULL) THEN
              RAISE EXCEPTION ''expression "NULL" cannot be used as an assignment target'';
            ELSIF (self.state = 0) THEN
              RAISE EXCEPTION ''incorrect usage of method GetCount'';
            END IF;
            return self.count;
          end;

          MEMBER FUNCTION GETTYPE(self IN pg_catalog.ANYDATASET, typ OUT pg_catalog.AnyType) RETURN INT AS
          begin
            IF (self IS NULL) THEN
              RAISE EXCEPTION ''expression "NULL" cannot be used as an assignment target'';
            ELSIF (self.state = 0) THEN
              RAISE EXCEPTION ''incorrect usage of method GETTYPE'';
            END IF;
            return self.typecode;
          end;

          MEMBER FUNCTION GETTYPENAME(self IN pg_catalog.ANYDATASET) RETURN VARCHAR2 AS
          begin
            IF (self IS NULL) THEN
              RAISE EXCEPTION ''expression "NULL" cannot be used as an assignment target'';
            ELSIF (self.state = 0) THEN
              RAISE EXCEPTION ''incorrect usage of method GETTYPENAME'';
            END IF;
            return self.type_name;
          end;
        END;';
  end if;
END$$;
