package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangql on 8/13/15.
 */
public class BasicTest {

    private static Configuration conf = null;
    private static HConnection conn = null;

    static {
        try {
            conf = HBaseConfiguration.create();
            conn = HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close(){
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 示例：createTable("table1", new String[] { "cf1", "cf2" });
     *
     * @param tableName
     * @param columnFamilies
     * @return 创建是否成功
     */
    public static boolean createTable(String tableName, String[] columnFamilies) {
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
            if (hBaseAdmin.tableExists(tableName)) {
                hBaseAdmin.close();
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(
                    TableName.valueOf(tableName));
            for (String cfName : columnFamilies) {
                tableDescriptor.addFamily(new HColumnDescriptor(cfName));
            }
            hBaseAdmin.createTable(tableDescriptor);
            hBaseAdmin.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean tableExists(String tableName) throws IOException {
        boolean bExists = false;
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        if (hBaseAdmin.tableExists(tableName))
            bExists = true;
        hBaseAdmin.close();
        return bExists;
    }

    /**
     * 指定一个cell插入数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnQualifier
     * @param value
     * @return 插入是否成功
     */
    public static boolean insertData(String tableName, String rowKey,
                              String columnFamily, String columnQualifier, byte[] value) {
        try {
            HTableInterface table = conn.getTable(tableName);
            Put put = new Put(rowKey.getBytes());
            put.add(columnFamily.getBytes(), columnQualifier.getBytes(), value);
            table.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获得row key前缀为 rowKeyPrefix 的所有行对应列的value列表
     *
     * @param tableName
     * @param rowKeyPrefix
     * @param columnFamily
     * @param columnQualifier
     * @return List of all values
     */
    public static List<byte[]> scanData(String tableName, String rowKeyPrefix,
                                 String columnFamily, String columnQualifier) {
        byte[] bytes             = rowKeyPrefix.getBytes();
        bytes[bytes.length - 1] += 1;
        String endRow            = new String(bytes);
        return scanData(tableName, rowKeyPrefix, endRow, columnFamily, columnQualifier);
    }

    /**
     * 获得row key在[startRow, endRow)内所有行对应列的value列表
     *
     * @param tableName
     * @param startRow
     * @param endRow
     * @return List of all values
     */
    public static List<byte[]> scanData(String tableName, String startRow,
                                 String endRow, String columnFamily, String columnQualifier) {
        List<byte[]> values = new ArrayList<byte[]>();
        try {
            HTableInterface table = conn.getTable(tableName);
            Scan scan = new Scan(startRow.getBytes(), endRow.getBytes());
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                byte[] value = r.getValue(columnFamily.getBytes(),
                        columnQualifier.getBytes());
                values.add(value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return values;
    }

    /**
     * 指定一个cell，获得最新版本的数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnQualifier
     * @return value
     */
    public static byte[] getData(String tableName, String rowKey, String columnFamily,
                          String columnQualifier) {
        try {
            HTableInterface table = conn.getTable(tableName);
            Get get = new Get(rowKey.getBytes());
            Result r = table.get(get);
            return r.getValue(columnFamily.getBytes(),
                    columnQualifier.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void dropTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Object[] insertMany(String tableName, List<String> rowKeys, String columnFamily, String columnQualifier, List<byte[]> values) {
        try {
            HTableInterface table = conn.getTable(tableName);
            List<Put> puts = new ArrayList<Put>(values.size());
            for (int i = 0; i < values.size(); i++){
                Put p = new Put(rowKeys.get(i).getBytes());
                p.add(columnFamily.getBytes(), columnQualifier.getBytes(), values.get(i));
                puts.add(p);
            }
            Object[] results = new Object[values.size()];
            table.batch(puts, results);
            table.close();
            return results;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }



    public static void main(String[] args) throws IOException {
        String tableName = "readTest";
        String[] cfs = {"cf1", "cf2"};
        String row = "12345";
        String inputName = "/home/huangql/tmp/hbase/lines";

        List<String> rowKeys = new ArrayList<String>();
        List<byte[]> values = new ArrayList<byte[]>();
        String col = "";
        BufferedReader r = new BufferedReader(new FileReader(inputName));
        String line;
        while ((line = r.readLine()) != null){
            String[] ss = line.split("\t");
            rowKeys.add(ss[0]);
            col = ss[1];
            values.add(ss[2].getBytes());
        }

        StopWatch w = new StopWatch("total time used:");
        w.start();
        insertMany(tableName+"1", rowKeys, cfs[0], col, values);
        w.stop();
        w.printProfiling();

        w.clear();
        w.start();
        for (int i = 0; i < values.size(); i++){
            insertData(tableName+"2", rowKeys.get(i), cfs[0], col, values.get(i));
        }
        w.stop();
        w.printProfiling();

//        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
//        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
//        for (String cfName : cfs) {
//            tableDescriptor.addFamily(new HColumnDescriptor(cfName).setMaxVersions(10));
//        }
//        hBaseAdmin.createTable(tableDescriptor);
//        hBaseAdmin.close();
//
//        for (int i = 0; i < 10; i++) {
//            insertData(tableName, row, cfs[0], "0", ("time"+i).getBytes());
//        }

//        Get get = new Get(row.getBytes());
//        get.addFamily(cfs[0].getBytes());
//        get.setMaxVersions(20);
//        HTableInterface table = conn.getTable(tableName);
//        Result r = table.get(get);
//        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> m = r.getMap();
//        for (byte[] cf : m.keySet()){
//            NavigableMap<byte[], NavigableMap<Long, byte[]>> cm = m.get(cf);
//            for (byte[] cq: cm.keySet()){
//                NavigableMap<Long, byte[]> sm = cm.get(cq);
//                for (long ts : sm.keySet()) {
//                    System.out.println(new String(cq) + " " + ts + " " + new String(sm.get(ts)));
//                }
//            }
//        }

        //dropTable(tableName);

        close();
    }
}
