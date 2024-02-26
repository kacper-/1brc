package com.km;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Locale;
import java.util.Random;

public class CreateFile {
    private static final int SIZE = 100000000;
    private static final int ID_COUNT = 10000;
    private static final String SEPARATOR = ";";

    public static void main(String[] args) {
        String[] ids = new String[ID_COUNT];
        for (int i = 0; i < ID_COUNT; i++)
            ids[i] = String.format("id%04d", i);

        File file = new File("./input4.txt");
        try {
            BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(file));
            int j;
            float temp;
            for (int i = 0; i < SIZE; i++) {
                j = new Random().nextInt(ID_COUNT);
                temp = new Random().nextInt(999);
                temp /= 10;
                if (new Random().nextBoolean())
                    temp = -temp;
                writer.write(String.format(Locale.US, "%s%s%.1f\n", ids[j], SEPARATOR, temp).getBytes());
            }
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
