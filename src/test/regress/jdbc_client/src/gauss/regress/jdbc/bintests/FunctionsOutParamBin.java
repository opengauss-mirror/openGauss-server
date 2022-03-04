package gauss.regress.jdbc.bintests;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class FunctionsOutParamBin  implements IBinaryTest {

	@Override
	public void execute(DatabaseConnection4Test conn) {
		String sql;
		BinUtils.createCLSettings(conn);

		sql = "CREATE TABLE t_processed (name text, val INT ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC), val2 INT)";
		conn.executeSql(sql);
		sql = "INSERT INTO t_processed VALUES('name', 1, 2)";
		conn.executeSql(sql);
		conn.fetchData("select * from t_processed");
		conn.executeSql("CREATE TABLE t_processed_b (name text, val bytea ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC), val2 INT)"); 
		conn.executeSql("INSERT INTO t_processed_b VALUES('name', 'test', 2)");
				
		sql = "CREATE OR REPLACE FUNCTION f_processed_out_1param(out1 OUT int) AS 'SELECT val from t_processed LIMIT 1' LANGUAGE SQL";
		conn.executeSql(sql);
		conn.executeSql("CREATE OR REPLACE FUNCTION f_processed_out(out1 OUT int, out2 OUT int) AS 'SELECT val, val2 from t_processed LIMIT 1' LANGUAGE SQL");
		conn.executeSql("CREATE OR REPLACE FUNCTION f_processed_out_b(out1 OUT bytea, out2 OUT int) AS 'SELECT val, val2 from t_processed_b LIMIT 1' LANGUAGE SQL");
		conn.executeSql("CREATE OR REPLACE FUNCTION f_processed_out_plpgsql(out out1 int, out out2 int)\n" + 
				"as $$\n" + 
				"begin\n" + 
				"  select val, val2 INTO out1, out2 from t_processed;\n" + 
				"end;$$\n" + 
				"LANGUAGE plpgsql");
		conn.executeSql("CREATE OR REPLACE FUNCTION f_processed_out_plpgsql2(out out1 t_processed.val%TYPE, out out2 t_processed.val%TYPE)\n" + 
				"as $$\n" + 
				"begin\n" + 
				"  select val, val2 INTO out1, out2 from t_processed;\n" + 
				"end;$$\n" + 
				"LANGUAGE plpgsql");
		conn.executeSql("CREATE OR REPLACE FUNCTION f_processed_aliases_plpgsql(out out1 int, out out2 int) as\n" + 
				"$BODY$\n" + 
				"DECLARE\n" + 
				" val1 ALIAS FOR out1;\n" + 
				"begin\n" + 
				"  select val, val2 INTO val1, out2 from t_processed;\n" + 
				"end;\n" + 
				"$BODY$\n" + 
				"LANGUAGE plpgsql");
		conn.fetchData("select f_processed_out_1param()");
		conn.fetchData("select f_processed_out()");
		conn.fetchData("select f_processed_out_b()");
		conn.fetchData("select f_processed_out_plpgsql()");
		conn.fetchData("select f_processed_out_plpgsql2()");
		conn.fetchData("select f_processed_aliases_plpgsql()");

		conn.executeSql("drop function f_processed_out_1param");
		conn.executeSql("drop function f_processed_out");
		conn.executeSql("drop function f_processed_out_b");
		conn.executeSql("drop function f_processed_out_plpgsql");
		conn.executeSql("drop function f_processed_out_plpgsql2");
		conn.executeSql("drop function f_processed_aliases_plpgsql");
		conn.executeSql("drop table t_processed");
		conn.executeSql("drop table t_processed_b");
		BinUtils.dropCLSettings(conn);
	}
}
