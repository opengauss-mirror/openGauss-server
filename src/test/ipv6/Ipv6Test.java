/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/test/ipv6/Ipv6Test.java
 *
 * ---------------------------------------------------------------------------------------
 */
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.text.SimpleDateFormat;
import org.postgresql.util.*;

public
class Ipv6Test {
public
    static Connection GetConnection(String port,String host)
    {
        String urls = "jdbc:postgresql://" + host + ":" + port + "/postgres?prepareThreshold=0&loggerLevel=off";
        String driver = "org.postgresql.Driver";

        Properties urlProps = new Properties();
        urlProps.setProperty("user", "ipv6_tmp");
        urlProps.setProperty("password", "Gauss@123");

        Connection conn = null;
        try {
            Class.forName(driver).newInstance();
            conn = DriverManager.getConnection(urls, urlProps);
            System.out.println("Connection succeed!");
        } catch (Exception exception) {
            exception.printStackTrace();
            return null;
        }

        return conn;
    };


public
    static void CreateTable(Connection conn)
    {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();

            int drc = stmt.executeUpdate("drop table if exists ipv6_test ;");

            int rc = stmt.executeUpdate("create table ipv6_test(id int, class int, name text, score float);");


            stmt.close();
        } catch (SQLException exception) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException exception1) {
                    exception1.printStackTrace();
                }
            }
            exception.printStackTrace();
        }
    }


public
    static void main(String[] args)
    {
        String PORT = args[0];
	String HOST = args[1];
        Connection conn = GetConnection(PORT,HOST);

        if (conn == null) {
            System.out.println("connection failed");
            return;
        }
        CreateTable(conn);

        try {
            conn.close();
            System.out.println("close connection");
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }
}
