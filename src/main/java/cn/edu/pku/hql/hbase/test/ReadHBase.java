package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by huangql on 9/1/15.
 */
public class ReadHBase {

    public static void main(String[] args) throws IOException {
        HConnection conn = HConnectionManager.createConnection(HBaseConfiguration.create());

        String tableName = "tuning_event_0831";
        HTableInterface table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("hotel"));
        scan.setBatch(100);

        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();

        BufferedWriter writer = new BufferedWriter(new FileWriter("hotel100.events"));
        while (it.hasNext()) {
            byte[] cell = it.next().getValue(Bytes.toBytes("f"), Bytes.toBytes("hotel"));
            String json = new String(cell);
            writer.write(json + "\n");
        }
        writer.close();
    }
}
