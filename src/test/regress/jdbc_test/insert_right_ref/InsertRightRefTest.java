import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * test Case for insert stament with right value references
 */
public class InsertRightRefTest {
    private static final String DEFAULT_DRIVER_NAME = "org.postgresql.Driver";
    private static final String DEFAULT_DATABASE = "regression";
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_PORT = "";
    private static final String DEFAULT_USER_NAME = "";
    private static final String DEFAULT_PASSWORD = "";
    private static final int MAX_ROW = 50;

    private final Connection con;
    private final StringBuilder outputBuffer;

    InsertRightRefTest(Connection con) {
        this.con = con;
        outputBuffer = new StringBuilder();
    }

    /**
     * main programme
     *
     * @param args args[0]: host
     *             args[1]: port
     *             args[2]: database
     *             args[3]: user name
     *             args[4]: password
     */
    public static void main(String[] args) {
        try (Connection con = getConnection(args)) {
            new InsertRightRefTest(con).runTestCases();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    static Connection getConnection(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName(DEFAULT_DRIVER_NAME);
        String host = (args != null && args.length > 0) ? args[0] : DEFAULT_HOST;
        String port = (args != null && args.length > 1) ? args[1] : DEFAULT_PORT;
        String database = (args != null && args.length > 2) ? args[2] : DEFAULT_DATABASE;
        String name = (args != null && args.length > 3) ? args[3] : DEFAULT_USER_NAME;
        String password = (args != null && args.length > 4) ? args[4] : DEFAULT_PASSWORD;
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database + "?prepareThreshold=0&loggerLevel=off";

        return DriverManager.getConnection(url, name, password);
    }

    void runTestCases() {
        // case 1: base test
        execute("drop table if exists auto_increment_t;");
        execute("create table auto_increment_t(n int, c1 int primary key auto_increment, c2 int, c3 int);");
        execute("insert into auto_increment_t values(1, c1, c1, c1);");
        String[] batch = {"insert into auto_increment_t values(2, 0, c1, c1);",
                "insert into auto_increment_t values(3, 0, c1, c1);",
                "insert into auto_increment_t values(4, -1, c1, c1);",
                "insert into auto_increment_t(n, c2, c3, c1) values(5, c1, c1, 1000);",
                "insert into auto_increment_t values(5, c1, c1, c1);"};
        executeBatch(batch);
        select("select * from auto_increment_t order by n;", "n", "c1", "c2", "c3");
        execute("drop table auto_increment_t;");

        execute("create table char_default_t(\n" +
                "    n serial,\n" +
                "    c1 char(10) default 'char20',\n" +
                "    c2 char(10),\n" +
                "    c3 varchar(10) default 'vc3',\n" +
                "    c4 varchar(20),\n" +
                "    c5 varchar2(10) default 'vc210',\n" +
                "    c6 varchar2(20),\n" +
                "    c7 nchar(5) default 'c31',\n" +
                "    c8 nchar(5),\n" +
                "    c9 nvarchar2(5) default 'c33',\n" +
                "    c10 nvarchar(5) default 'c34',\n" +
                "    c11 varchar(20) default concat('hello', ' world'),\n" +
                "    c12 varchar(20)\n" +
                ");");
        execute("insert into char_default_t values(1);\n" +
                "insert into char_default_t values(2, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12);\n" +
                "insert into char_default_t values(3, c1, c2, c3, concat(c3, ' vc4'), c5, c6, c7, c8, c9, c10, default, c11);");
        select("select * from char_default_t;",
                "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12");
        execute("drop table if exists char_default_t");

        // case 2: upsert
        execute("drop table if exists upser;");
        execute("create table upser(c1 int, c2 int, c3 int);");
        execute("create unique index idx_upser_c1 on upser(c1);");
        executeBatch("insert into upser values (1, 10, 10), (2, 10, 10), (3, 10, 10), (4, 10, 10), (5, 10, 10),\n" +
                        " (6, 10, 10), (7, 10, 10), (8, 10, 10), (9, 10, 10), (10, 10, 10);",
                "insert into upser values (5, 100, 100), (6, 100, 100), (7, 100, 100), (8, 100, 100),(9, 100, 100),\n" +
                        "    (10, 100, 100), (11, 100, 100), (12, 100, 100), (13, 100, 100), (14, 100, 100), (15, 100, 100)\n" +
                        "    on duplicate key update c2 = c1 + c2, c3 = c2 + c3;");
        select("select * from upser order by c1;", "c1", "c2", "c3");
        executeBatch("truncate upser;",
                "insert into upser values (1, 10, 10), (2, 10, 10), (3, 10, 10), (4, 10, 10), (5, 10, 10), \n" +
                        "(6, 10, 10), (7, 10, 10), (8, 10, 10), (9, 10, 10), (10, 10, 10);",
                "insert into upser values (5, c1 + 100, 100), (6, c1 + 100, 100), (7, c1 + 100, 100), \n" +
                        "(8, c1 + 100, 100), (9, c1 + 100, 100), (10, c1 + 100, 100), (11, c1 + 100, 100),\n" +
                        "(12, c1 + 100, 100), (13, c1 + 100, 100), (14, c1 + 100, 100), (15, c1 + 100, c1 + c2)\n" +
                        "on duplicate key update c2 = c1 + c2, c3 = c2 + c3;");
        select("select * from upser order by c1;", "c1", "c2", "c3");
        execute("drop table upser;");

        // case 3: var & function
        execute("drop table if exists with_var;");
        execute("drop function if exists with_var_func;");
        execute("create table with_var(a int default 999);");
        execute("create function with_var_func() return int as\n" +
                "declare \n" +
                "    a int := 666;\n" +
                "begin\n" +
                "    insert into with_var values(a);\n" +
                "    return a;\n" +
                "end;\n" +
                "/");
        executeBatch("call with_var_func();",
                "call with_var_func();",
                "call with_var_func();");
        select("select * from with_var;", "a");
        executeBatch("drop function with_var_func;",
                "drop table with_var;");
        // case 4: custum types
        execute("create table custom_notnull_t(\n" +
                "    c0 TestEnum3 not null,    \n" +
                "    c1 TestEnum not null,\n" +
                "    c2 TestEnum2 not null,\n" +
                "    c3 TestCom not null,\n" +
                "    c4 TestCom2 not null,\n" +
                "    c5 int[] not null,\n" +
                "    c6 blob[][] not null\n" +
                ");");
        execute("insert into custom_notnull_t values(c0, c1, c2, c3, c4, c5, c6);");
        select("select * from custom_notnull_t;", "c0", "c1", "c2", "c3", "c4", "c5", "c6");
        // case 5: set, enum
        execute("create table enum_set_notnull_t(\n" +
                "    c1 TestEnum  not null,\n" +
                "    c2 TestEnum2 not null,\n" +
                "    c3 set('666') not null,\n" +
                "    c4 set('hello', 'world') not null\n" +
                ");\n" +
                "insert into enum_set_notnull_t values(c1, c2, c3, c4);");
        select("select * from enum_set_notnull_t;", "c1", "c2", "c3", "c4");
    }

    void select(String sql, String... fields) {
        System.out.println(sql);
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int[] maxWidths = new int[fields.length];
            for (int i = 0; i < fields.length; i++) {
                maxWidths[i] = fields[i].length();
            }

            int cnt = 0;
            List<String[]> data = new ArrayList<>();
            while (cnt++ < MAX_ROW && rs.next()) {
                String[] row = new String[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    row[i] = rs.getString(fields[i]);
                    if (row[i] == null) {
                        row[i] = "?_?";
                    }
                    if (row[i].length() > maxWidths[i]) {
                        maxWidths[i] = row[i].length();
                    }
                }
                data.add(row);
            }

            printRow(maxWidths, fields);
            printSeparator(maxWidths);
            for (String[] row : data) {
                printRow(maxWidths, row);
            }
            if (data.size() == 1) {
                System.out.println("(1 row)");
            } else {
                System.out.println("(" + data.size() + " rows)");
            }
            System.out.println();
        } catch (SQLException exception) {
            System.err.println(exception.getMessage());
        }
    }

    void execute(String sql) {
        System.out.println(sql);
        try (Statement stmt = con.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException exception) {
            System.err.println(exception.getMessage());
        }
    }

    void executeBatch(String... sqls) {
        try (Statement stmt = con.createStatement()) {
            for (String sql : sqls) {
                stmt.addBatch(sql);
                System.out.println(sql);
            }
            stmt.executeBatch();
        } catch (SQLException exception) {
            System.err.println(exception.getMessage());
        }
    }

    void printRow(int[] maxWidths, String[] values) {
        outputBuffer.append("|");
        for (int i = 0; i < maxWidths.length; i++) {
            outputBuffer.append(String.format(" %-" + maxWidths[i] + "s |", values[i]));
        }
        System.out.println(outputBuffer);
        outputBuffer.delete(0, outputBuffer.length());
    }

    void printSeparator(int[] maxWidths) {
        for (int maxWidth : maxWidths) {
            String format = String.format("+-%-" + maxWidth + "s-", '-').replaceAll(" ", "-");
            outputBuffer.append(format);
        }
        outputBuffer.append("+");
        System.out.println(outputBuffer);
        outputBuffer.delete(0, outputBuffer.length());
    }
}
