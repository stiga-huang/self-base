package cn.edu.pku.hql.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Test for the minimum config set to read from HDFS using federation
 *
 * Created by quanlong.huang on 29/01/2018.
 */
public class ConfigTest {
  private static Configuration getConf() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://warehousestore");
    conf.set("dfs.nameservices", "warehousestore");
    conf.set("dfs.client.failover.proxy.provider.warehousestore", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    conf.set("dfs.ha.namenodes.warehousestore", "namenode144,namenode700");
    conf.set("dfs.namenode.rpc-address.warehousestore.namenode144", "xxx:8020");
    conf.set("dfs.namenode.rpc-address.warehousestore.namenode700", "yyy:8020");
    return conf;
  }

  public static void main(String[] args) throws IOException {
    FileSystem fs = FileSystem.get(getConf());
    FileStatus[] list = fs.listStatus(new Path("/hive"));
    for (FileStatus f : list) {
      System.out.println(f.getPath());
    }
  }
}
