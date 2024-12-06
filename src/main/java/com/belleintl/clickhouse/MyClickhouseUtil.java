package com.belleintl.clickhouse;

import com.belleintl.finkbean.Mail;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.PreparedStatement;

/**
 * @ClassName: MyClickhouseUtil
 * @Description:
 * @Author: zhipengl01
 * @Date: 2022/6/1
 */
public class MyClickhouseUtil extends RichSinkFunction<Mail> {
    private ClickHouseConnection conn = null;

    private String sql;

    public MyClickhouseUtil(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
    }

    @Override
    public void invoke(Mail mail, Context context) throws Exception {
        String url = "jdbc:clickhouse://10.10.30.51:8123";
        ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setUser("lin.zp");
        clickHouseProperties.setPassword("S65yNvdR18");

        // 用properties和url来构造一个连接Clickhouse的DataSource
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, clickHouseProperties);

        try {
            // 从该连接池获取连接conn
            conn = dataSource.getConnection();
            // 利用JDBC的prepareStatement，来对写好的SQL中的占位符进行赋值
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString(1, mail.getAppKey());
            preparedStatement.setString(2, mail.getAppVersion());
            preparedStatement.setString(3, mail.getAppKey());
            preparedStatement.setString(4, mail.getPhoneNo());

            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
