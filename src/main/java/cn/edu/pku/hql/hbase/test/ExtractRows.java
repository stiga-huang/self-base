package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.io.BufferedWriter;
import java.io.FileWriter;

/**
 * Created by huangql on 9/15/15.
 */
public class ExtractRows {

    public static Scan getScan(int batchSize) {
        System.out.println("scan wich cache size: " + batchSize);
        Scan scan = new Scan();
        if (batchSize > 0) {
            // set scanner caching: the number of rows which are cached
            // before returning the result to the client
            scan.setCaching(batchSize);
        }
        // each row will only be read one time, so disable server-side block caching
        scan.setCacheBlocks(false);
        // only return the first cell in each row.
        scan.setFilter(new FirstKeyOnlyFilter());
        return scan;
    }

    public static void run(String tableName, String fileName, int batch) throws Exception {
        try (BufferedWriter output = new BufferedWriter(new FileWriter(fileName))) {

            Configuration conf = HBaseConfiguration.create();
            long t1 = System.currentTimeMillis();
            try (HConnection conn = HConnectionManager.createConnection(conf)) {
                System.out.println("connect time used: " + (System.currentTimeMillis() - t1));
                t1 = System.currentTimeMillis();

                try (HTableInterface table = conn.getTable(tableName)) {
                    System.out.println("get table time: " + (System.currentTimeMillis() - t1));
                    t1 = System.currentTimeMillis();

                    Scan scan = getScan(batch);
                    int count = 0;
                    try (ResultScanner rs = table.getScanner(scan)) {
                        for (Result r : rs) {
                            output.write(new String(r.getRow()) + "\n");
                            count++;
                            if (count % 100000 == 0) {
                                System.out.println(count + "rows");
                            }
                        }
                    }

                    System.out.println("total scan time: " + (System.currentTimeMillis() - t1));
                    System.out.println("total rows: " + count);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: tableName outputFileName [batchSize]");
            System.exit(1);
        }
        int batch = 0;
        if (args.length >= 3) {
            batch = Integer.parseInt(args[2]);
        }
        run(args[0], args[1], batch);
    }
}
