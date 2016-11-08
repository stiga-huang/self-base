package cn.edu.pku.hql.phoenix.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by huangql on 11/5/15.
 */
public class RandomBatchReadTest {

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

                StringBuffer sql = new StringBuffer("SELECT * FROM \"" + tableName
                        + "\" WHERE \"id\" IN (");  // 在Phoenix中统一定义row key为id
                for (int i = 0; i < batch; i++) {
                    sql.append("?,");
                }
                sql.setCharAt(sql.length()-1, ')');
                PreparedStatement stmt = conn.prepareStatement(sql.toString());

                while (true) {
                    long timeUsed = 0;
                    int i;
                    for (i = 0; i < batch; i++) {
                        String line = reader.readLine();
                        if (line == null) break;
                        stmt.setString(i+1, line);
                    }
                    if (i == batch) {
                        t1 = System.currentTimeMillis();
                        ResultSet rs = stmt.executeQuery();
                        timeUsed += System.currentTimeMillis() - t1;
                        System.out.println("get " + i + ", time used(ms): " + timeUsed);
                    } else if (i < batch)
                        break;
                }
            }
        }
    }

}
