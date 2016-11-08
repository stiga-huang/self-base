package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangql on 9/15/15.
 */
public class RandomBatchGetTest {

    private static final Logger logger = Logger.getLogger(RandomBatchGetTest.class);

    public static void run(String tableName, String rowKeyFile, int batch) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(rowKeyFile))) {

            Configuration conf = HBaseConfiguration.create();
            long t1 = System.currentTimeMillis();
            try (HConnection conn = HConnectionManager.createConnection(conf)) {
                //System.out.println("connect time used: " + (System.currentTimeMillis() - t1));
                logger.info("connect time used(ms): " + (System.currentTimeMillis() - t1));

                t1 = System.currentTimeMillis();
                try (HTableInterface table = conn.getTable(tableName)) {
                    //System.out.println("get table time: " + (System.currentTimeMillis() - t1));
                    logger.info("get table time(ms): " + (System.currentTimeMillis() - t1));

                    List<Get> gets = new ArrayList<>();
                    while (true) {
                        int i;
                        for (i = 0; i < batch; i++) {
                            String line = reader.readLine();
                            if (line == null) break;
                            Get get = new Get(Bytes.toBytes(line));
                            gets.add(get);
                        }

                        if (i > 0) {
                            if (batch == 1) {
                                t1 = System.nanoTime();
                                table.get(gets.get(0));
                                logger.info("get " + i + ", time used(ms): " + (System.nanoTime() - t1) / 1000000.0);
                            } else {
                                t1 = System.currentTimeMillis();
                                table.get(gets);
                                logger.info("get " + i + ", time used(ms): " + (System.currentTimeMillis() - t1));
                            }
                            gets.clear();
                        }
                        if (i < batch)  break;
                    }
                }
            }
        }
    }

    public static void firstConnTest(String tableName, String rowKeyFile, int batch) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(rowKeyFile));
            PrintWriter connStat = new PrintWriter(new BufferedWriter(new FileWriter("conn.stat")));
            PrintWriter tableStat = new PrintWriter(new BufferedWriter(new FileWriter("table.stat")));
            PrintWriter getStat = new PrintWriter(new BufferedWriter(new FileWriter("get.stat")))) {

            Configuration conf = HBaseConfiguration.create();
            List<Get> gets = new ArrayList<>();
            while (true) {
                long t1 = System.currentTimeMillis();
                long timeUsed;
                try (HConnection conn = HConnectionManager.createConnection(conf)) {
                    timeUsed = System.currentTimeMillis() - t1;
                    logger.info("connect time used(ms): " + timeUsed);
                    connStat.println(timeUsed);

                    t1 = System.currentTimeMillis();
                    try (HTableInterface table = conn.getTable(tableName)) {
                        timeUsed = System.currentTimeMillis() - t1;
                        logger.info("get table time(ms): " + timeUsed);
                        tableStat.println(timeUsed);

                        int i;
                        for (i = 0; i < batch; i++) {
                            String line = reader.readLine();
                            if (line == null) break;
                            Get get = new Get(Bytes.toBytes(line));
                            gets.add(get);
                        }

                        t1 = System.currentTimeMillis();
                        table.get(gets);
                        timeUsed = System.currentTimeMillis() - t1;
                        logger.info("get " + i + ", time used(ms): " + timeUsed);
                        getStat.println(timeUsed);
                        gets.clear();
                        if (i < batch)  break;
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: tableName rowKeyFile batch");
            System.exit(1);
        }

        Logger.getRootLogger().setLevel(Level.INFO);

        run(args[0], args[1], Integer.parseInt(args[2]));
        //firstConnTest(args[0], args[1], Integer.parseInt(args[2]));
    }
}
