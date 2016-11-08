package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Pair;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huangql on 11/5/15.
 */
public class ExtractRowsMT {

    public volatile AtomicInteger totalCount = new AtomicInteger(0);

    class Worker implements Runnable {

        byte[] startKey, endKey;
        int batch;
        HTable table;
        PrintWriter writer;

        public Worker(HTable table, byte[] startKey, byte[] endKey, int batch, PrintWriter writer) {
            this.table = table;
            this.startKey = startKey;
            this.endKey = endKey;
            this.batch = batch;
            this.writer = writer;
        }

        @Override
        public void run() {
            String id = Thread.currentThread().getName();
            System.out.println(id + " start");

            System.out.println(id + " scan wich cache size=" + batch + ", start="
                    + new String(startKey) + ", end=" + new String(endKey));
            Scan scan = new Scan(startKey, endKey);
            if (batch > 0) {
                // set scanner caching: the number of rows which are cached
                // before returning the result to the client
                scan.setCaching(batch);
            }
            // each row will only be read one time, so disable server-side block caching
            scan.setCacheBlocks(false);
            // only return the first cell in each row.
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner scanner = null;
            try {
                scanner = table.getScanner(scan);
            } catch (IOException e) {
                System.err.println(id + " failed");
                e.printStackTrace();
                return;
            }

            int count = 0;
            for (Result r : scanner) {
                writer.println(new String(r.getRow()));
                count++;
                if (count % 100000 == 0) {
                    System.out.println(id + ": scan " + count + " rows");
                }
            }
            System.out.println(id + ": scan " + count + " rows");

            totalCount.addAndGet(count);
            System.out.println(id + " finished");
        }
    }

    public void run(String tableName, String fileName, int batch) throws Exception {

        File dir = new File(fileName);
        if (!dir.mkdirs()) {
            System.err.println("mkdir " + fileName + " failed.");
            return;
        }

        long timeUsed = 0;
        long t1 = System.currentTimeMillis();
        Configuration conf = HBaseConfiguration.create();

        try (HTable table = new HTable(conf, tableName)) {
            Pair<byte[][], byte[][]> pairs = table.getStartEndKeys();
            byte[][] startKeys = pairs.getFirst();
            byte[][] endKeys = pairs.getSecond();
            timeUsed += System.currentTimeMillis() - t1;

            int num = startKeys.length;
            PrintWriter writers[] = new PrintWriter[num];
            Thread threads[] = new Thread[num];
            for (int i = 0; i < startKeys.length; i++) {
                writers[i] = new PrintWriter(new BufferedWriter(new FileWriter(fileName + '/' + i)));
                threads[i] = new Thread(new Worker(table, startKeys[i], endKeys[i], batch, writers[i]));
            }
            System.out.println("prepare time used(ms): " + timeUsed);
            System.out.println("start all threads. thread num = " + num);
            t1 = System.currentTimeMillis();
            for (Thread t : threads)
                t.start();
            for (Thread t : threads)
                t.join();
            timeUsed += System.currentTimeMillis() - t1;

            for (PrintWriter w : writers) {
                if (w != null)
                    w.close();
            }
            System.out.println("total row keys: " + totalCount);
            System.out.println("total time used(ms): " + timeUsed);
            System.out.println("finished");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: tableName outputDirName [batchSize]");
            System.exit(1);
        }
        int batch = 0;
        if (args.length >= 3) {
            batch = Integer.parseInt(args[2]);
        }
        new ExtractRowsMT().run(args[0], args[1], batch);
    }
}
