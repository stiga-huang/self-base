package cn.edu.pku.hql.mapreduce.test;

import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.tools.rumen.JobConfigurationParser;
import org.apache.hadoop.tools.rumen.JobHistoryParser;
import org.apache.hadoop.tools.rumen.JobHistoryParserFactory;
import org.apache.hadoop.tools.rumen.RewindableInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * An example to parse a job history for which the version is not
 * known i.e using JobHistoryParserFactory.getParser()
 */
public class HistoryParserTest {

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: fileName");
            System.exit(1);
        }
        String fileName = args[0];
        InputStream in = new FileInputStream(fileName);

        if (fileName.endsWith(".xml")) {
            Properties properties = new Properties();
            properties.loadFromXML(in);

        } else if (fileName.endsWith(".hist")) {
            RewindableInputStream ris = new RewindableInputStream(in);

            // JobHistoryParserFactory will check and return a parser that can
            // parse the file
            JobHistoryParser parser = JobHistoryParserFactory.getParser(ris);

            // now use the parser to parse the events
            HistoryEvent event = parser.nextEvent();
            while (event != null) {
                System.out.println(event.getEventType());
                event = parser.nextEvent();
            }

            // close the parser and the underlying stream
            parser.close();
        }

    }
}
