package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by huangql on 11/13/15.
 */
public class WriteTest {

    public static void main(String[] args) throws IOException {

        HConnection conn = HConnectionManager.createConnection(HBaseConfiguration.create());
        HTableInterface table = conn.getTable("test");
        Put put = new Put(Bytes.toBytes("1234"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes('a'), Bytes.toBytes(1234));
        put.add(Bytes.toBytes("f"), Bytes.toBytes('b'), Bytes.toBytes("1234"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("a"), Bytes.toBytes(16464));
        table.put(put);
        table.close();
        conn.close();
    }
}
