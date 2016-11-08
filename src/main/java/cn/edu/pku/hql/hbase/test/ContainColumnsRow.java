package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by huangql on 9/17/15.
 */
public class ContainColumnsRow {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: tableName columnCount col1 col2 ...");
            System.exit(1);
        }
        String tableName = args[0];
        int colNum = Integer.parseInt(args[1]);
        System.out.println("table: " + tableName + ", columnNum: " + colNum);

        String cf[] = new String[colNum];
        String column[] = new String[colNum];
        for (int i = 0; i < colNum; i++){
            String[] ss = args[i + 2].split(":");
            cf[i] = ss[0];
            column[i] = ss[1];
            System.out.print(" " + cf[i] + ":" + column[i]);
        }
        System.out.println();

        Configuration conf = HBaseConfiguration.create();
        try (HConnection conn = HConnectionManager.createConnection(conf)) {
            try (HTableInterface table = conn.getTable(tableName)) {
                Scan scan = new Scan();
                scan.setCaching(1000);
                scan.setCacheBlocks(false);

                ResultScanner rs = table.getScanner(scan);
                int count = 0;
                for (Result r : rs) {
                    count++;
                    boolean containsAll = true;
                    for (int i = 0; i < colNum; i++) {
                        if (!r.containsColumn(Bytes.toBytes(cf[i]), Bytes.toBytes(column[i]))) {
                            containsAll = false;
                            break;
                        }
                    }
                    if (!containsAll) continue;

                    System.out.println("--------------------------------------------------------");
                    System.out.println("row = " + new String(r.getRow()));
                    CellScanner cs = r.cellScanner();
                    while (cs.advance()) {
                        Cell c = cs.current();
                        byte[] f = CellUtil.cloneFamily(c);
                        byte[] cq = CellUtil.cloneQualifier(c);
                        byte[] value = CellUtil.cloneValue(c);
                        System.out.println("cf = " + new String(f) + ", cq = " + new String(cq));
                        System.out.println("value = " + new String(value));
                    }
                }
                rs.close();

                System.out.println("scan " + count + " rows");
            }
        }
    }
}
