package cn.edu.pku.hql.basic.test;

public class OverflowBehavior {
  public static void main(String[] args) {
    long[] ndvs = {14951,4,37148544,37148544,37148544,37148544,37148544,37148544,37148544,1907944,1907944,1907944,1907944,1907944,1907944,1907944};
    long res = 1;
    for (long ndv : ndvs) {
      long old = res;
      res *= ndv;
      System.out.printf("%d * %d = %d%n", old, ndv, res);
    }
    System.out.println("res = " + res);

    long[] ndvs2 = {14951,4,37148545,37148545,37148545,37148545,37148545,37148545,37148545,1907945,1907945,1907945,1907945,1907945,1907945,1907945};
    long res2 = 1;
    for (long ndv : ndvs2) {
      long old = res2;
      res2 *= ndv;
      System.out.printf("%d * %d = %d%n", old, ndv, res2);
    }
    System.out.println("res = " + res2);
  }
}
