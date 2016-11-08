package cn.edu.pku.hql.hbase.test;

/**
 * Created by huangql on 8/15/15.
 */
public class StopWatch {
    // time unit: ms
    private long startTime = 0;
    private long endTime = 0;
    private long totalTime = 0;
    private String message = "";

    public StopWatch(String prefixMsg) {
        message += prefixMsg;
    }

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void stop() {
        endTime = System.currentTimeMillis();
        assert endTime > startTime;
        totalTime += endTime - startTime;
    }

    public void clear() {totalTime = 0; startTime = 0; endTime = 0;}

    public void printProfiling() {
        System.out.println(message + ": " + totalTime + "ms");
    }
}
