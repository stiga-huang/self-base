package cn.edu.pku.hql.basic.test;

public class Utf8Strings {
  public static void main(String[] args) {
    String s = "哈";
    System.out.println(s.length());
    System.out.println(s.getBytes().length);
    s = "hello";
    System.out.println(s.length());
    System.out.println(s.getBytes().length);

    s = "北京市海淀区";
    int cityCodePoint = '市';  // A single quoted Chinese character. It's value is 24066.
    int a = 0x5E02;
    System.out.println(a);
    System.out.println(s.substring(s.indexOf(cityCodePoint) + 1));
    System.out.println(s.substring(3));

    s = "grüßen";
    System.out.println(s.toUpperCase());
    System.out.println("grüßen".toUpperCase());
    System.out.println("ὈΔΥΣΣΕΎΣ".toLowerCase());
  }
}
