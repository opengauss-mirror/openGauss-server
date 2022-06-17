--test create function/procedure definer=user 
drop procedure IF EXISTS  mysqlschema.definer();
drop procedure IF EXISTS  mysqlschema.invoker();
CREATE  DEFINER=usr2 PROCEDURE mysqlschema.definer() SECURITY DEFINER
AS
BEGIN
   raise info 'create definer procedure.';
END;
/

CREATE DEFINER=usr2 PROCEDURE mysqlschema.invoker() SECURITY INVOKER
AS
BEGIN
   raise info 'create invoker procedure.';
END;
/
