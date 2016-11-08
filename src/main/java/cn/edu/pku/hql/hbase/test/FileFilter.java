package cn.edu.pku.hql.hbase.test;

import org.apache.commons.math3.util.MathArrays;

import java.io.*;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by huangql on 9/15/15.
 */
public class FileFilter {

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("usage: fileName totalLines resLines");
            System.exit(1);
        }
        String fileName = args[0];
        int totalLines = Integer.parseInt(args[1]);
        int resLines = Integer.parseInt(args[2]);

        int[] lineNum = new int[resLines];
        for (int i = 0; i < resLines; i++) {
            lineNum[i] = new Random().nextInt(totalLines);
        }
        System.out.println("sorting...");
        Arrays.sort(lineNum);

        String[] row = new String[resLines];
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            int count = 0;
            int resCount = 0;
            System.out.println("reading...");
            while (true) {
                line = reader.readLine();
                if (line == null)   break;
                if (count == lineNum[resCount]) {
                    row[resCount] = line;
                    resCount++;
                    if (resCount == resLines)
                        break;
                }
                count++;
            }
        }

        int[] index = new int[resLines];
        for (int i = 0; i < resLines; i++) {
            index[i] = i;
        }
        System.out.println("shuffling...");
        MathArrays.shuffle(index);

        System.out.println("writing...");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("resLines"))) {
            for (int i = 0; i < resLines; i++) {
                writer.write(row[index[i]] + "\n");
            }
        }

    }
}
