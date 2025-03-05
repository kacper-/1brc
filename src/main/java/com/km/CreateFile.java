package com.km;

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

public class CreateFile {
    private static final int SIZE = 1000000000;
    private static final int ID_COUNT = 10000;
    private static final char SEPARATOR = ';';
    private static final char POINT = '.';

    public static void main(String[] args) {
        String[] ids = new String[ID_COUNT];
        for (int i = 0; i < ID_COUNT; i++)
            ids[i] = String.format("id%04d", i);

        File file = new File("./input4.txt");
        try {
            long start = new Date().getTime();
            BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(file));
            int j;
            int temp;
            StringBuilder sb = new StringBuilder();
            Random random = new Random();
            for (int i = 0; i < SIZE; i++) {
                j = random.nextInt(ID_COUNT);
                temp = random.nextInt(999);
                if ((temp & 8) == 8)
                    temp = -temp;
                sb.append(ids[j]);
                sb.append(SEPARATOR);
                sb.append(temp / 10);
                sb.append(POINT);
                sb.append(Math.abs(temp % 10));
                sb.append('\n');
                if((j & 128) == 128) {
                    writer.write(sb.toString().getBytes());
                    sb = new StringBuilder();
                }
            }
            writer.close();
            long stop = new Date().getTime();
            System.out.println(stop - start);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
