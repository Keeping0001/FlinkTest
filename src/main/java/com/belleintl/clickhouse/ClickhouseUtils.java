package com.belleintl.clickhouse;

import java.sql.*;

/**
 * @ClassName: ClickhouseUtils
 * @Description: java通过jdbc连接clickhouse
 * @Author: zhipengl01
 * @Date: 2021/12/3
 */
public class ClickhouseUtils {
    private static Connection connection = null;

    static {
        try {
//            Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            String url = "jdbc:clickhouse://10.10.30.51:8123";
            String user = "lin.zp";
            String password = "S65yNvdR18";

            connection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from ldp_dwd.dwd_wms_im_putaway_info_dd limit 10");
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();
        while(resultSet.next()) {
            for(int i=1; i <= columnCount; i++) {
                System.out.println(metaData.getColumnName(i) + ":" + resultSet.getString(i));
                System.out.println(metaData.getSchemaName(i));
            }
        }
    }
}
