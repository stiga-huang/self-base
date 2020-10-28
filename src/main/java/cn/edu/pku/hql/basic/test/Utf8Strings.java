package cn.edu.pku.hql.basic.test;

public class Utf8Strings {
  public static void main(String[] args) {
    String s = "哈";
    System.out.println(s.length());
    System.out.println(s.getBytes().length);
    s = "hello";
    System.out.println(s.length());
    System.out.println(s.getBytes().length);
  }
}
