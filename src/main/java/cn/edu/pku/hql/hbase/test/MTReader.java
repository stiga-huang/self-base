package cn.edu.pku.hql.hbase.test;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Process file input in multi-thread
 *
 * Created by huangql on 9/21/15.
 */
public class MTReader implements Runnable {

    private static final Logger logger = Logger.getLogger(MTReader.class);
    private static BufferedReader reader;
    private static int counter = 0;

    private static synchronized String readLine() throws IOException {
        String line = reader.readLine();
        counter++;
        logger.info(counter + ": " + line);
        return line;
    }

    @Override
    public void run() {
        try {
            String line;
            while ((line = readLine()) != null) {
                //logger.info(line);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        String fileName = args[0];
        MTReader.reader = new BufferedReader(new FileReader(fileName));

        Thread ts[] = new Thread[5];
        for (int i = 0; i < 5; i++) {
            ts[i] = new Thread(new MTReader());
            ts[i].start();
        }

        for (int i = 0; i < 5; i++) {
            ts[i].join();
        }

        MTReader.reader.close();
    }

}
