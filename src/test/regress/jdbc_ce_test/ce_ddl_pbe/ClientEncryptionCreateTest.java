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
import java.util.*;
public class ClientEncryptionCreateTest {

    public static void main(String[] args) throws IOException {
        // prepare connect info
	    String PORT = args[0];
        String urls = "jdbc:postgresql://localhost:"+ PORT +"/regression?enable_ce=1";
        Properties urlProps = new Properties();
        urlProps.setProperty("user", "test");
        urlProps.setProperty("password", "Gauss@123");

        // create connection
        int conNumber = 2;
        Connection[] cons = new Connection[conNumber];
        try{
            for (int i = 0; i < conNumber; i++) {
                cons[i] = DriverManager.getConnection(urls, urlProps);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }

        Connection con;
        Statement st;
        PreparedStatement pst;
        int i = 0;
        int j = 0;
        try {
            con = cons[0];
            UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS CreateCMK1 CASCADE;");
            UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS CreateCMK CASCADE;");
            UtilTool.executeSql(con, "CREATE CLIENT MASTER KEY CreateCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/1\" , ALGORITHM = AES_256_CBC);");
            UtilTool.executeSql(con, "CREATE CLIENT MASTER KEY CreateCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/2\" , ALGORITHM = AES_256_CBC);");
            UtilTool.executeSql(con, "CREATE COLUMN ENCRYPTION KEY CreateCEK1 WITH VALUES (CLIENT_MASTER_KEY = CreateCMK1, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256);");
            UtilTool.executeSql(con, "CREATE COLUMN ENCRYPTION KEY CreateCEK WITH VALUES (CLIENT_MASTER_KEY = CreateCMK, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256);");
            System.out.println("ok");
            //数值类型
            pst = con.prepareStatement("drop table if exists t1;");
            pst.executeUpdate();
            System.out.println("drop table if exists t1;");
             try { 
         System.out.println(new Date( ) + "\n"); 
         Thread.sleep(1000*30);   // 休眠3秒
         System.out.println(new Date( ) + "\n"); 
      } catch (Exception e) { 
          System.out.println("Got an exception!"); 
      }
            pst = con.prepareStatement("create table t1(c0 int, c1 TINYINT default ?, c2 SMALLINT default ?, c3 INTEGER default 1 + ?, c4 BINARY_INTEGER default ?, c5 BIGINT default ?, c6 DECIMAL(10,4) default ?, c7 NUMERIC(10,4) default ?, c8 int default ?, c9 int default ?, c10 int default ?, c11 FLOAT4 default ?, c12 FLOAT8 default ?, c13 FLOAT(3) default ?, c14 BINARY_DOUBLE default ?, c15 DEC(10,4) default ?, c16 INTEGER(6,3) default ?);");
            pst.setInt(1, 123);
            pst.setInt(2, 100);
            pst.setInt(3, 4 + 5 + 6);
            pst.setInt(4, 123);
            pst.setInt(5, 123);
            pst.setString(6, "147258.14");
            pst.setString(7, "12369.01");
            pst.setString(8, "12");
            pst.setString(9, "12");
            pst.setString(10, "23");
            pst.setString(11, "10.365456");
            pst.setString(12, "123456.1234");
            pst.setString(13, "10.3214");
            pst.setString(14, "321.321");
            pst.setString(15, "123.123654");
            pst.setString(16, "123.123654");
            pst.executeUpdate();
            System.out.println("create table t1"); 

            //字符类型
            //pst = con.prepareStatement("drop table if exists t2;");
           // pst.executeUpdate();

          /*  try { 
         System.out.println(new Date( ) + "\n"); 
         Thread.sleep(1000*30);   // 休眠3秒
         System.out.println(new Date( ) + "\n"); 
      } catch (Exception e) { 
          System.out.println("Got an exception!"); 
      }*/
            System.out.println("drop table if exists t2;");
            pst = con.prepareStatement("create table t2(c0 int, c1 char(4) default ? , c2 varchar(5) default ?, c3 VARCHAR2(100) default ?,c4 NVARCHAR2(100) default ?, c5 CLOB default ?, c6 TEXT default ? encrypted with (column_encryption_key = CreateCEK, encryption_type = DETERMINISTIC));");
            pst.setString(1, "TRUE");
            pst.setString(2, "FALSE");
            pst.setString(3, "d's你好'd你好asd");
            pst.setString(4, "你好sdsd");
            pst.setString(5, "s%s/'adas");
            pst.setString(6, "sad''你好'sada");
            pst.executeUpdate();

            //布尔类型
            pst = con.prepareStatement("drop table if exists t3;");
            pst.executeUpdate();
            pst = con.prepareStatement("create table t3(c0 int, c1 BOOLEAN default ? , c2 BOOLEAN default ?, c3 BOOLEAN default ?);");
            pst.setBoolean(1, true);
            pst.setBoolean(2, false);
            pst.setString(3, "1");
            pst.executeUpdate();

            //不影响正常建表
            pst = con.prepareStatement("drop table if exists t4;");
            pst.executeUpdate();
            pst = con.prepareStatement("create table t4(c0 int, c1 BOOLEAN default true, c2 int default 1, c3 varchar default 'abc' );");
            pst.executeUpdate();

            //alter table
            pst = con.prepareStatement("drop table if exists t5;");
            pst.executeUpdate();
            pst = con.prepareStatement("create table t5(c0 int,  c2 int default 1);");
            pst.executeUpdate();
            pst = con.prepareStatement("alter table t5 add column c3 varchar default ? ;");
			pst.setString(1, "abc");
            pst.executeUpdate();

            pst = con.prepareStatement("drop table if exists t6;");
            pst.executeUpdate();
            pst = con.prepareStatement("create table t6(c0 int, c1 BOOLEAN default true, c2 int default 1);");
            pst.executeUpdate();
            pst = con.prepareStatement("alter table t6 alter column c2 set default ?;");
			pst.setInt(1, 2);
            pst.executeUpdate();

            pst.close();

        } catch (SQLException e) {
            System.out.println(i);
            System.out.println(j);
            e.printStackTrace();
            System.exit(0);
        }

        // close all connection;
        try {
            for (i = 0; i < conNumber; i++) {
                cons[i].close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}

