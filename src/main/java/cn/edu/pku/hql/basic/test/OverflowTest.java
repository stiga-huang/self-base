package cn.edu.pku.hql.basic.test;

/**
 * Created by huangql on 7/30/16.
 */
public class OverflowTest {
    public static void main(String[] args) {
        int a = Integer.MIN_VALUE;
        System.out.println(a);
        System.out.println(-a);
        System.out.println(a == -a);
    }
}
