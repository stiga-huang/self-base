package cn.edu.pku.hql.basic.test;

import org.apache.log4j.*;

/**
 * Created by huangql on 1/14/16.
 */
public class Log4jTest {
    public static void main(String[] args) {
        //BasicConfigurator.configure();
        Logger root = Logger.getRootLogger();
        root.removeAllAppenders();
        root.setLevel(Level.WARN);
        //root.addAppender(new ConsoleAppender(new PatternLayout("%r [%t] %p %c %x - %m%n")));
        root.addAppender(new ConsoleAppender(
                new PatternLayout("%-d{yyyy-MM-dd HH:mm:ss} %-4r [%t] %-5p %c %x - %m%n")));

        root.debug("debug");
        root.info("info");
        root.warn("warn");
        root.error("error");
    }
}
