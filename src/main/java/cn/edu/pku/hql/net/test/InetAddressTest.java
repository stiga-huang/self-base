package cn.edu.pku.hql.net.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by quanlong.huang on 1/21/17.
 */
public class InetAddressTest {

  public static void main(String[] args) throws UnknownHostException {
    if (args.length < 1) {
      System.err.println("Args: host1 host2 ...");
      System.exit(1);
    }
    for (String host : args) {
      System.out.println("Resolving host of '" + host + "'");
      InetAddress resolvedAddresses[] = InetAddress.getAllByName(host);
      for (InetAddress address : resolvedAddresses) {
        System.out.println(address);
      }
    }
  }
}
