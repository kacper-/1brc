package com.km;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class Main {
    private static final int SIZE = 100000;
    private static ByteBuffer bb;
    private static final Map<String, float[]> map = new HashMap<>();
    private static int j = 0;
    private static int sep = 0;
    private static final char[] arr = new char[110];
    private static final byte[] buffer = new byte[SIZE];

    public static void main(String[] args) {
        int pos;
        try {
            long start = new Date().getTime();
            RandomAccessFile file = new RandomAccessFile("./input4.txt", "r");
            FileChannel channel = file.getChannel();
            bb = ByteBuffer.allocateDirect(SIZE);

            while (channel.read(bb) > -1) {
                pos = bb.position();
                bb.get(0, buffer, 0, pos);
                bb.position(0);
                readBuffer(pos);
            }

            channel.close();
            file.close();

            PriorityQueue<String> pq = new PriorityQueue<>(Comparator.naturalOrder());
            pq.addAll(map.keySet());

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
            String key;
            float[] ff;
            while ((key = pq.poll()) != null) {
                ff = map.get(key);
                writer.write(key + "\t" + ff[0] + "\t" + ff[1] / ff[3] + '\t' + ff[2] + '\n');
            }

            writer.flush();

            long stop = new Date().getTime();
            System.out.println("execution time = " + (stop - start));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void readBuffer(int pos) {
        for (int i = 0; i < pos; i++) {
            arr[j] = (char) buffer[i];
            if (buffer[i] == 59) {
                sep = j;
            }
            if (buffer[i] == 10) {
                String key = new String(arr, 0, sep);
                float f = getFloat(sep, j - 1);

                if (map.containsKey(key)) {
                    float[] ff = map.get(key);
                    ff[0] = Math.min(f, ff[0]);
                    ff[1] += f;
                    ff[2] = Math.max(f, ff[2]);
                    ff[3]++;
                    map.put(key, ff);
                } else
                    map.put(key, new float[]{f, f, f, 1f});

                j = -1;
            }
            j++;
        }
    }

    private static float getFloat(int from, int to) {
        float val = 0f;
        int c = 0;
        int d;
        for (int i = to; i > from; i--) {
            d = arr[i] - 48;
            if (d == -3)
                return -val / 10;
            if (d > -1) {
                switch (c) {
                    case 0:
                        val = d;
                        break;
                    case 1:
                        val += 10 * d;
                        break;
                    case 2:
                        val += 100 * d;
                }
                c++;
            }
        }
        return val / 10;
    }
}
