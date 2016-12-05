package cn.edu.pku.hql.titan;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Utils used in titan tests
 *
 * Created by huangql on 11/28/16.
 */
public class Util {

    private static final String uselessInfoLogs[] = {
            "org.apache.zookeeper",
            "org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper",
            "org.apache.hadoop.hbase.client"
    };
    public static void suppressUselessInfoLogs() {
        for (String prefix : uselessInfoLogs)
            Logger.getLogger(prefix).setLevel(Level.WARN);
    }

    public static String getTitanHBaseTableName(String titanConf) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(titanConf));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("storage.hbase.table")) {
                return line.substring(line.indexOf('=') + 1);
            }
        }
        return "Not Set!!!";
    }
}
