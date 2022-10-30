package javatest01.TwoPhaseCommitTest;

/**
 * @author jiangfan
 * @date 2022/9/30 11:54
 */
import com.alibaba.druid.pool.DruidDataSourceFactory;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DruidConnectionPool {

    private transient static DataSource dataSource = null;
    private transient static Properties props = new Properties();

    // 静态代码块
    static {
        System.out.println("jingtai code~~~~~~~~~~~");
        props.put("driverClassName", "com.mysql.jdbc.Driver");
        props.put("url", "jdbc:mysql://localhost:3306/mytest_db?characterEncoding=utf8");
        props.put("username", "root");
        props.put("password", "123456");
        try {
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private DruidConnectionPool() {
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}

