package com.huangshang.demo.jstorm.kafka.util;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangshang on 2018/9/2 下午1:15.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class MySQLUtil {
    private static String className = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://127.0.0.1:3306/db_treasurebox?useUnicode=true&characterEncoding=utf-8";
    private static String user = "root";
    private static String password = "123456";
    private static QueryRunner queryRunner = new QueryRunner();

    public static final String INSERT_LOG = "INSERT INTO kafka_msg(msg,create_at) VALUES(?,?)";
    public static final String ADD_ZCY_COUNT = "UPDATE demo_msg_analyze_t SET zcy_count = zcy_count + 1";
    public static final String ADD_ERROR_COUNT = "UPDATE demo_msg_analyze_t SET error_count = error_count + 1";

    static{
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void update(String sql, Object... params) throws SQLException {
        Connection connection = getConnection();
        //更新数据
        queryRunner.update(connection, sql, params);

        connection.close();
    }

    public static List<String> executeQuerySql(String sql) {

        List<String> result = new ArrayList<String>();
        try {
            List<Object[]> requstList = queryRunner.query(getConnection(), sql,
                    new ArrayListHandler(new BasicRowProcessor() {
                        @Override
                        public <Object> List<Object> toBeanList(ResultSet rs,
                                                                Class<Object> type) throws SQLException {
                            return super.toBeanList(rs, type);
                        }
                    }));
            for (Object[] objects : requstList) {
                result.add(objects[0].toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
    /**
     * @throws SQLException
     *
     */
    public static Connection getConnection() throws SQLException {
        //获取mysql连接
        return DriverManager.getConnection(url, user, password);
    }

}
