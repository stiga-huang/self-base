package cn.edu.pku.hql.basic.test;

import java.net.URL;

/**
 * Created by huangql on 6/26/16.
 */
public class ClassLoaderTest {
    public static void main(String[] args) {
        URL[] urls = sun.misc.Launcher.getBootstrapClassPath().getURLs();
        for (URL url : urls) {
            System.out.println(url.toExternalForm());
        }
    }
}
