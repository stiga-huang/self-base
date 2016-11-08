package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by huangql on 1/15/16.
 */
public class AtomicWriteTest {

    static class UnSafeWriter implements Runnable {

        Object lock;
        HTableInterface table;
        byte[] row, family, column;
        int inc;

        public UnSafeWriter(Object lock, HTableInterface table, String row,
                            String family, String column, int inc) {
            this.lock = lock;
            this.table = table;
            this.row = Bytes.toBytes(row);
            this.family = Bytes.toBytes(family);
            this.column = Bytes.toBytes(column);
            this.inc = inc;
        }

        @Override
        public void run() {
            String name = Thread.currentThread().getName();
            synchronized (lock) {
                try {
                    System.out.println(name + " run, start to wait");
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println(name + " wake up");
            try {
                Result result = table.get(new Get(row));
                int value = Bytes.toInt(result.getValue(family, column));
                int newValue = value + inc;
                Put put = new Put(row);
                put.add(family, column, Bytes.toBytes(newValue));
                table.put(put);

                System.out.println(name + " got " + value + ", writed " + newValue);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class SafeWriter implements Runnable {
        Object lock;
        HTableInterface table;
        byte[] row, family, column;
        int inc;

        public SafeWriter(Object lock, HTableInterface table, String row,
                            String family, String column, int inc) {
            this.lock = lock;
            this.table = table;
            this.row = Bytes.toBytes(row);
            this.family = Bytes.toBytes(family);
            this.column = Bytes.toBytes(column);
            this.inc = inc;
        }

        @Override
        public void run() {
            String name = Thread.currentThread().getName();
            synchronized (lock) {
                try {
                    System.out.println(name + " run, start to wait");
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println(name + " wake up");
            try {
                while (true) {
                    Result result = table.get(new Get(row));
                    byte[] value = result.getValue(family, column);
                    int newValue = Bytes.toInt(value) + inc;
                    Put put = new Put(row);
                    put.add(family, column, Bytes.toBytes(newValue));
                    if (table.checkAndPut(row, family, column, value, put)) {
                        System.out.println(name + " got " + Bytes.toInt(value) + ", writed " + newValue);
                        break;
                    } else {
                        System.out.println(name + " write failed, try again");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static int getResult(HTableInterface table, String row, String family, String column) {
        try {
            Result result = table.get(new Get(Bytes.toBytes(row)));
            return Bytes.toInt(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void UnSafeWriteTest() throws IOException {
        String tableName = "atomicTest";
        String row = "row1";
        String family = "f";
        String column = "a";
        int inc = 1;

        Configuration conf = HBaseConfiguration.create();
        try (HConnection conn = HConnectionManager.createConnection(conf)) {

            // since HTableInterface is not thread-safe, we need two HTableInterface
            try (HTableInterface table1 = conn.getTable(tableName);
                 HTableInterface table2 = conn.getTable(tableName)) {

                System.out.println("init result = " + getResult(table1, row, family, column));

                Object lock = new Object();
                Thread writer1 = new Thread(new UnSafeWriter(lock, table1, row, family, column, inc));
                Thread writer2 = new Thread(new UnSafeWriter(lock, table2, row, family, column, inc));

                writer1.start();
                writer2.start();

                Thread.sleep(100);
                synchronized (lock) {
                    lock.notifyAll();
                }

                writer1.join();
                writer2.join();

                System.out.println("final result = " + getResult(table1, row, family, column));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void SafeWriteTest() throws IOException {
        String tableName = "atomicTest";
        String row = "row1";
        String family = "f";
        String column = "a";
        int inc = 1;

        Configuration conf = HBaseConfiguration.create();
        try (HConnection conn = HConnectionManager.createConnection(conf)) {

            // since HTableInterface is not thread-safe, we need two HTableInterface
            try (HTableInterface table1 = conn.getTable(tableName);
                 HTableInterface table2 = conn.getTable(tableName)) {

                System.out.println("init result = " + getResult(table1, row, family, column));

                Object lock = new Object();
                Thread writer1 = new Thread(new SafeWriter(lock, table1, row, family, column, inc));
                Thread writer2 = new Thread(new SafeWriter(lock, table2, row, family, column, inc));

                writer1.start();
                writer2.start();

                Thread.sleep(100);
                synchronized (lock) {
                    lock.notifyAll();
                }

                writer1.join();
                writer2.join();

                System.out.println("final result = " + getResult(table1, row, family, column));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        //UnSafeWriteTest();
        SafeWriteTest();
    }
}
