package cn.edu.pku.hql.basic.test;

/**
 * Measure time cost in System.currentTimeMills()
 *
 * Created by huangql on 12/13/16.
 */
public class MethodTimeCost {
    public static void main(String[] args) {
        long t1, t2;
        long invokeTime, emptyTime;

        t1 = System.nanoTime();
        System.currentTimeMillis();
        t2 = System.nanoTime();
        invokeTime = t2 - t1;
        System.out.println(invokeTime);

        t1 = System.nanoTime();
        t2 = System.nanoTime();
        emptyTime = t2 - t1;
        System.out.println(emptyTime);

        System.out.println("Time spend in System.currentTimeMills(): " +
                (invokeTime - emptyTime) + " ns");
    }
}
