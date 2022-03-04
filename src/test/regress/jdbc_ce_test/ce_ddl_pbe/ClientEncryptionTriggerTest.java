import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.sql.*;

public class ClientEncryptionTriggerTest {
    public static void testTrigger(Connection conn) throws SQLException {
        String[] sqls = {
                "CREATE TABLE test_trigger_src_tbl(id1 INT, id2 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = triggerCEK1, ENCRYPTION_TYPE = DETERMINISTIC), id3 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = triggerCEK2, ENCRYPTION_TYPE = DETERMINISTIC));",
                "CREATE TABLE test_trigger_des_tbl(id1 INT, id2 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = triggerCEK1, ENCRYPTION_TYPE = DETERMINISTIC), id3 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = triggerCEK2, ENCRYPTION_TYPE = DETERMINISTIC));",
                "CREATE OR REPLACE FUNCTION tri_insert_func() RETURNS TRIGGER AS\n" +
                        "            $$\n" +
                        "            DECLARE\n" +
                        "            BEGIN\n" +
                        "            INSERT INTO test_trigger_des_tbl VALUES(NEW.id1, NEW.id2, NEW.id3);\n" +
                        "            RETURN NEW;\n" +
                        "            END\n" +
                        "            $$ LANGUAGE PLPGSQL;",
                "CREATE OR REPLACE FUNCTION tri_update_func() RETURNS TRIGGER AS\n" +
                        "            $$\n" +
                        "            DECLARE\n" +
                        "            BEGIN\n" +
                        "            UPDATE test_trigger_des_tbl SET id3 = NEW.id3 WHERE id2=OLD.id2;\n" +
                        "            RETURN OLD;\n" +
                        "            END\n" +
                        "            $$ LANGUAGE PLPGSQL;",
                "CREATE OR REPLACE FUNCTION TRI_DELETE_FUNC() RETURNS TRIGGER AS\n" +
                        "            $$\n" +
                        "            DECLARE\n" +
                        "            BEGIN\n" +
                        "            DELETE FROM test_trigger_des_tbl WHERE id2=OLD.id2;\n" +
                        "            RETURN OLD;\n" +
                        "            END\n" +
                        "            $$ LANGUAGE PLPGSQL;",
                "CREATE TRIGGER insert_trigger BEFORE INSERT ON test_trigger_src_tbl FOR EACH ROW EXECUTE PROCEDURE tri_insert_func();",
                "CREATE TRIGGER update_trigger AFTER UPDATE ON test_trigger_src_tbl FOR EACH ROW EXECUTE PROCEDURE tri_update_func();",
                "CREATE TRIGGER delete_trigger BEFORE DELETE ON test_trigger_src_tbl FOR EACH ROW EXECUTE PROCEDURE tri_delete_func();",
                "INSERT INTO test_trigger_src_tbl VALUES(100,200,300);",
                "SELECT * FROM test_trigger_src_tbl;",
                "SELECT * FROM test_trigger_des_tbl;",

        };
        for (int i = 0; i < sqls.length - 2; i++) {
            UtilTool.executeSql(conn, sqls[i]);
        }
        for (int i = sqls.length - 2; i < sqls.length; i++) {
            UtilTool.fetchData(conn, sqls[i]);
        }
        UtilTool.executeSql(conn, "UPDATE test_trigger_src_tbl SET id3=400 WHERE id2=200;");
        for (int i = sqls.length - 2; i < sqls.length; i++) {
            UtilTool.fetchData(conn, sqls[i]);
        }
        UtilTool.executeSql(conn, "DELETE FROM test_trigger_src_tbl WHERE id2=200;");
        for (int i = sqls.length - 2; i < sqls.length; i++) {
            UtilTool.fetchData(conn, sqls[i]);
        }
        System.out.println("OK (1 test)");
    }
    public static void main(String[] args) throws IOException {
        // prepare connect info
	    String PORT = args[0];
        String urls = "jdbc:postgresql://localhost:"+ PORT +"/regression?enable_ce=1";
        Properties urlProps = new Properties();

        // create connection
        Connection con = null;
        try {
            urlProps.setProperty("user", "test");
            urlProps.setProperty("password", "Gauss@123");
            con = DriverManager.getConnection(urls, urlProps);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }

        try {
            UtilTool.executeSql(con, "drop table if exists test_trigger_src_tbl;");
            UtilTool.executeSql(con, "drop table if exists test_trigger_des_tbl;");
            UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS triggerCMK CASCADE;");
            UtilTool.executeSql(con, "CREATE CLIENT MASTER KEY triggerCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/1\" , ALGORITHM = AES_256_CBC);");
            UtilTool.executeSql(con, "CREATE COLUMN ENCRYPTION KEY triggerCEK1 WITH VALUES (CLIENT_MASTER_KEY = triggerCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);");
            UtilTool.executeSql(con, "CREATE COLUMN ENCRYPTION KEY triggerCEK2 WITH VALUES (CLIENT_MASTER_KEY = triggerCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);");
            testTrigger(con);
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}