package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangql on 1/14/16.
 */
public class DeleteWithFilter {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HConnection conn = HConnectionManager.createConnection(conf);
        HTableInterface table = conn.getTable("instanceTest");

        byte[] row = Bytes.toBytes("configuration");
        byte[] cf = Bytes.toBytes("s");
        byte[] colPrefix = Bytes.toBytes("system-registration");

        Get g = new Get(row);
        g.addFamily(cf);
        g.setFilter(new ColumnPrefixFilter(colPrefix));

        List<Delete> deleteList = new ArrayList<>();
        Result result = table.get(g);
        CellScanner cs = result.cellScanner();
        while (cs.advance()) {
            byte[] qualifier = CellUtil.cloneQualifier(cs.current());
            Delete d = new Delete(row);
            d.deleteColumn(cf, qualifier);
            deleteList.add(d);
        }
        table.delete(deleteList);

        table.close();
        conn.close();
    }
}
