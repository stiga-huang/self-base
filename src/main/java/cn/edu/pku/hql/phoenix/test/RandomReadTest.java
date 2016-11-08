package cn.edu.pku.hql.phoenix.test;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

/**
 * Use to compare with com.mininglamp.hbase.test.RandomBatchGetTest
 *
 * Created by huangql on 10/30/15.
 */
public class RandomReadTest {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: tableName rowKeyFile batch");
            System.exit(1);
        }

        String tableName = args[0];
        String rowKeyFile = args[1];
        int batch = Integer.parseInt(args[2]);

        try (BufferedReader reader = new BufferedReader(new FileReader(rowKeyFile))) {

            long t1 = System.currentTimeMillis();
            try (Connection conn = DriverManager.getConnection(
                     "jdbc:phoenix:k1222.mzhen.cn,k1223.mzhen.cn,k1230.mzhen.cn")) {
                System.out.println("connect time used: " + (System.currentTimeMillis() - t1));

                int i = 0;
                PreparedStatement stmt = conn.prepareStatement("SELECT * FROM \"" + tableName
                        + "\" WHERE \"id\" = ?");  // 在Phoenix中统一定义row key为id
                while (true) {
                    long timeUsed = 0;
                    for (i = 0; i < batch; i++) {
                        String line = reader.readLine();
                        if (line == null) break;
                        t1 = System.currentTimeMillis();
                        stmt.setString(1, line);
                        ResultSet rs = stmt.executeQuery();
                        /*while (rs.next()) {
                            String id = rs.getString(1);
                            String hotel = rs.getString(2);
                            String railway = rs.getString(3);
                            System.out.println(id + "|||" + hotel + "|||" + railway);
                        }*/
                        timeUsed += System.currentTimeMillis() - t1;
                    }
                    System.out.println("get " + i + ", time used(ms): " + timeUsed);

                    if (i < batch) break;
                }
            }
        }
    }
}
