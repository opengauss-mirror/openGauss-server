import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ClientEncryptionPrepareTest {
    public static void main(String[] args) throws SQLException {
        // prepare connect info
        String PORT = args[0];
        String serverAddress = "localhost";
        String databseName = "regression";
        String username = "test";
        String password = "Gauss@123";
        // 创建连接对象
        Connection con = null;
        String jdbcConnectionString =
                String.format("jdbc:postgresql://%s:%s/%s?enable_ce=1", serverAddress, PORT, databseName);
        try {
            con = DriverManager.getConnection(jdbcConnectionString, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }
        // CMK CEK测试
        System.out.println(
                "drop  table start------------------------------------------------------------------------\n    ");

        System.out.println("test------------------------------------------------------------------------\n    ");
        UtilTool.executeSql(con, "drop table if exists products;");
        System.out.println(
                "drop table end------------------------------------------------------------------------\n    ");
        System.out.println(
                "drop pre_ImgCMK start------------------------------------------------------------------------\n    ");
        UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS pre_ImgCMK1 CASCADE;");
        UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS pre_ImgCMK CASCADE;");
        System.out.println(
                "drop pre_ImgCMK end------------------------------------------------------------------------\n    ");

        UtilTool.executeSql(
                con,
                "CREATE CLIENT MASTER KEY pre_ImgCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/1\" ,"
                    + " ALGORITHM = AES_256_CBC);");
        System.out.println(
                "CREATE CLIENT MASTER KEY  pre_ImgCMK1 WITH ( KEY_STORE = ? , KEY_PATH = \"gs_ktool/2\" , ALGORITHM ="
                    + " ?);");

        PreparedStatement pstm =
                con.prepareStatement(
                        "CREATE CLIENT MASTER KEY  pre_ImgCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/2\""
                            + " , ALGORITHM = SM4);");
        pstm.executeUpdate();
        System.out.println(
                "create pre_ImgCMK1 end------------------------------------------------------------------------\n    ");

        System.out.println(
                "create pre_ImgCEK start------------------------------------------------------------------------\n"
                    + "    ");
        Statement st = con.createStatement();
        int rowsAffected = 0;
        rowsAffected =
                st.executeUpdate(
                        "CREATE COLUMN ENCRYPTION KEY pre_ImgCEK WITH VALUES (CLIENT_MASTER_KEY = pre_ImgCMK1,"
                            + " ALGORITHM  = SM4_sm3);");

        System.out.println(
                "create pre_ImgCEK end------------------------------------------------------------------------\n    ");
        String createtab_products =
                " CREATE TABLE products              (                           product_id INTEGER,        "
                    + " product_name VARCHAR2(60) encrypted with (column_encryption_key = pre_ImgCEK, encryption_type"
                    + " = DETERMINISTIC),       category VARCHAR2(60) );";
        PreparedStatement pstm1 = con.prepareStatement(createtab_products);
        /*
         * check isValid for client encrytion connection, should return true even though 
         * parsingerror in jdbc client encrytion routine
         */
        if (con.isValid(0)) {
            int rs1 = pstm1.executeUpdate();
        }

        try {
            st.close();
            pstm.close();
            pstm1.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}
