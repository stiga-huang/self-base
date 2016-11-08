package cn.edu.pku.hql.basic.test;

import java.io.*;
import java.util.Properties;

/**
 * Test for java.util.Properties
 *
 * Created by huangql on 8/31/15.
 */
public class PropertyTest {

    public static void main(String[] args) throws IOException {
        String fileName = args[0];
        System.out.println("fileName: " + fileName);
        InputStream in = new FileInputStream(fileName);
        Properties prop = new Properties();
        prop.load(new InputStreamReader(in));
        System.out.println(prop.getProperty("descriptionFormat"));
        in.close();

        System.out.println("--------------");
        in = new FileInputStream(fileName);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        while (true) {
            String line = reader.readLine();
            if (line == null)   break;
            System.out.println(line);
        }
        in.close();
    }
}
