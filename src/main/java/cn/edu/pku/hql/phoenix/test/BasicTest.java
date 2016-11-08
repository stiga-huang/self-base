package cn.edu.pku.hql.phoenix.test;

import java.sql.*;

/**
 * Basic usage of Phoenix
 *
 * Created by huangql on 10/29/15.
 */
public class BasicTest {

    public static void main(String[] args) throws SQLException {

        Connection conn = DriverManager.getConnection("jdbc:phoenix:k1222.mzhen.cn,k1223.mzhen.cn,k1230.mzhen.cn");

        System.out.println("autoCommit = " + conn.getAutoCommit());

        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS STOCK_SYMBOL (SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR)");
        conn.createStatement().execute("UPSERT INTO STOCK_SYMBOL VALUES ('CRV','SalesForce.com')");

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO STOCK_SYMBOL VALUES(?,?)");
        stmt.setString(1, "crv");
        stmt.setString(2, "SalseForce.com");
        stmt.execute();

        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM STOCK_SYMBOL");
        while (rs.next()) {
            String symbol = rs.getString(1);
            String company = rs.getString(2);
            System.out.println(symbol + '\t' + company);
        }

        conn.close();
        System.out.println("finish");
    }
}
