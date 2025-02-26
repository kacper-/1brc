package com.km;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final AtomicInteger fullTime = new AtomicInteger(0);
    private static final AtomicInteger callCounter = new AtomicInteger(0);
    private static final int SIZE = 100000000;
    private static ByteBuffer[] bb;
    private static ByteBuffer one;
    private static final ArrayList<Map<String, int[]>> map = new ArrayList<>();
    private static byte[][] buffer;
    private static int CPU_COUNT;

    public static void main(String[] args) {
        CPU_COUNT = Runtime.getRuntime().availableProcessors() - 2;
        System.out.println(CPU_COUNT);
        bb = new ByteBuffer[CPU_COUNT];
        buffer = new byte[CPU_COUNT][SIZE + 110];
        Thread[] threads = new Thread[CPU_COUNT];
        for (int i = 0; i < CPU_COUNT; i++) {
            bb[i] = ByteBuffer.allocate(SIZE);
            map.add(new HashMap<>());
        }
        one = ByteBuffer.allocate(1);
        int pos;
        int tCount;
        try {
            long start = new Date().getTime();
            RandomAccessFile file = new RandomAccessFile("/Users/kacper/repo/1brc/input4.txt", "r");
            FileChannel channel = file.getChannel();
            boolean read = true;
            while (read) {
                tCount = 0;
                for (int t = 0; t < CPU_COUNT; t++) {
                    if (channel.read(bb[t]) < 0) {
                        read = false;
                        break;
                    }
                    pos = bb[t].position();
                    bb[t].get(0, buffer[t], 0, pos);
                    if (channel.position() < channel.size() && buffer[t][pos - 1] != 10) {
                        for (int a = 0; a < 110; a++) {
                            channel.read(one);
                            byte b = one.get(0);
                            one.position(0);
                            buffer[t][pos] = b;
                            pos++;
                            if (b == 10)
                                break;
                        }
                    }
                    bb[t].position(0);
                    tCount++;
                    final int ft = t;
                    final int fpos = pos;
                    final Map<String, int[]> m = map.get(t);
                    threads[t] = new Thread(() -> readBuffer(fpos, ft, m));
                    threads[t].start();
                }
                for (int i = 0; i < tCount; i++)
                    threads[i].join();
            }

            channel.close();
            file.close();
            Map<String, int[]> full = new HashMap<>(map.get(0));
            for (int t = 1; t < CPU_COUNT; t++) {
                for (String key : map.get(t).keySet()) {
                    int[] val = map.get(t).get(key);
                    if (full.containsKey(key)) {
                        int[] oldVal = full.get(key);
                        full.put(key, new int[]{Math.min(oldVal[0], val[0]), oldVal[1] + val[1], Math.max(oldVal[2], val[2]), oldVal[3] + val[3]});
                    } else {
                        full.put(key, val);
                    }
                }
            }

            PriorityQueue<String> pq = new PriorityQueue<>(Comparator.naturalOrder());
            pq.addAll(full.keySet());

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
            String key;
            int[] ff;
            while ((key = pq.poll()) != null) {
                ff = full.get(key);
                writer.write(key + ' ' + ((float) ff[0]) / 10 + ' ' + ((float) ff[1]) / ((float) ff[3] * 10) + ' ' + ((float) ff[2]) / 10 + '\n');
            }

            writer.flush();

            long stop = new Date().getTime();
            System.out.println("execution time = " + (stop - start));
            System.out.println("full execution time = " + fullTime.get());
            System.out.println("number of calls = " + callCounter.get());
            System.out.println("average call time = " + (float) (fullTime.get()) / (float) (callCounter.get()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void readBuffer(int pos, int idx, Map<String, int[]> m) {
        long start = new Date().getTime();
        int ls = 0;
        int sep = 0;
        for (int i = 0; i < pos; i++) {
            if (buffer[idx][i] == 59) {
                sep = i;
            }
            if (buffer[idx][i] == 10) {
                String key = new String(buffer[idx], ls, sep - ls);
                int f = getInt(buffer[idx][i - 5], buffer[idx][i - 4], buffer[idx][i - 3], buffer[idx][i - 1]);

                if (m.containsKey(key)) {
                    int[] ff = m.get(key);
                    ff[0] = Math.min(f, ff[0]);
                    ff[1] += f;
                    ff[2] = Math.max(f, ff[2]);
                    ff[3]++;
                    m.put(key, ff);
                } else
                    m.put(key, new int[]{f, f, f, 1});

                ls = i + 1;
            }
        }
        long stop = new Date().getTime();
        fullTime.addAndGet((int) (stop - start));
        callCounter.incrementAndGet();
    }

    private static int getInt(int s, int p3, int p2, int p1) {
        int val = (p1 & 15) + 10 * (p2 & 15);
        if (p3 == 59)
            return val;
        if (p3 == 45)
            return -val;
        val += 100 * (p3 & 15);
        if (s == 45)
            return -val;
        return val;
    }
}
