package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * Created by huangql on 9/16/15.
 */
public class ResultTest {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: tableName row");
            System.exit(1);
        }
        String tableName = args[0];
        String row = args[1];
        //String family = args[2];
        //String qualifier = args[3];

        System.out.println("table: " + tableName);
        System.out.println("row: " + Arrays.toString(row.split("\u0001")));

        Configuration conf = HBaseConfiguration.create();
        try (HConnection conn = HConnectionManager.createConnection(conf)) {
            try (HTableInterface table = conn.getTable(tableName)) {
                Get get = new Get(Bytes.toBytes(row));
                Result r = table.get(get);
                CellScanner cs = r.cellScanner();
                while (cs.advance()) {
                    Cell c = cs.current();
                    System.out.println("column: " + new String(CellUtil.cloneQualifier(c)));
                    System.out.println("value: " + new String(CellUtil.cloneValue(c)));
                }
            }
        }
    }
}
