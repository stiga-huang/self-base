package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by huangql on 11/9/15.
 */
public class TuningEventScanTest {

    private static final Logger logger = Logger.getLogger(TuningEventScanTest.class);

    public static void run(String tableName, String gidFile) throws Exception  {
        try (BufferedReader reader = new BufferedReader(new FileReader(gidFile))) {

            Configuration conf = HBaseConfiguration.create();
            long t1 = System.currentTimeMillis();
            try (HConnection conn = HConnectionManager.createConnection(conf)) {
                logger.info("connect time used(ms): " + (System.currentTimeMillis() - t1));


                t1 = System.currentTimeMillis();
                try (HTableInterface table = conn.getTable(tableName)) {
                    logger.info("get table time(ms): " + (System.currentTimeMillis() - t1));

                    while (true) {
                        String gid = reader.readLine();
                        if (gid == null)   break;
                        t1 = System.currentTimeMillis();
                        gid = gid.trim();
                        Scan scan = new Scan(Bytes.toBytes(gid), Bytes.toBytes(gid + '0'));
                        ResultScanner rs = table.getScanner(scan);
                        int count = 0;
                        for (Result r : rs) {
                            count++;
                        }
                        logger.info("scan " + count + " results, time used(ms): " + (System.currentTimeMillis() - t1));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: tableName gidFile");
            System.exit(1);
        }

        Logger.getRootLogger().setLevel(Level.INFO);

        run(args[0], args[1]);
    }

}
