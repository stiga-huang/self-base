package cn.edu.pku.hql.basic.test;

import java.math.BigInteger;
import java.util.Scanner;

import static java.math.BigInteger.ONE;
import static java.math.BigInteger.TEN;

public class Main {

    private static int getDigit(BigInteger m, int k) {
        return m.toString().charAt(k) - '0';
    }

    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);

        BigInteger index = input.nextBigInteger();
        if (index.equals(BigInteger.ZERO)) {
            System.out.println("0");
            return;
        }
        BigInteger tenPow = TEN;
        BigInteger n = ONE;
        BigInteger sub;

        while (true) {
            sub = tenPow.subtract(TEN).divide(BigInteger.valueOf(9));
            BigInteger rangeBeg = tenPow.multiply(n).subtract(sub);
            if (index.compareTo(rangeBeg) < 0)
                break;
            n = n.add(ONE);
            tenPow = tenPow.multiply(TEN);
        }
        BigInteger rs[] = index.add(sub).divideAndRemainder(n);
        System.out.println(getDigit(rs[0], rs[1].intValue()));
    }
}
