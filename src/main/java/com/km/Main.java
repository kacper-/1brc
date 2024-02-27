package com.km;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class Main {
    private static AtomicInteger fullTime = new AtomicInteger(0);
    private static AtomicInteger callCounter = new AtomicInteger(0);
    private static final int SIZE = 1000000;
    private static ByteBuffer[] bb;
    private static ArrayList<Map<String, float[]>> map = new ArrayList<>();
    private static int[] j;
    private static int[] sep;
    private static char[][] arr;
    private static byte[][] buffer;
    private static int CPU_COUNT;
    private static Thread[] threads;

    public static void main(String[] args) {
        CPU_COUNT = 3;//Runtime.getRuntime().availableProcessors() / 3;
        bb = new ByteBuffer[CPU_COUNT];
        j = new int[CPU_COUNT];
        sep = new int[CPU_COUNT];
        buffer = new byte[CPU_COUNT][SIZE];
        threads = new Thread[CPU_COUNT];
        for (int i = 0; i < CPU_COUNT; i++) {
            bb[i] = ByteBuffer.allocateDirect(SIZE);
            j[i] = 0;
            sep[i] = 0;
            map.add(new HashMap<>());
        }
        arr = new char[CPU_COUNT][110];
        int pos;
        int tCount = 0;

        try {
            long start = new Date().getTime();
            RandomAccessFile file = new RandomAccessFile("/Users/kacper/repo/1brc/input4.txt", "r");
            FileChannel channel = file.getChannel();

            while (channel.read(bb) > -1) {
                for (int i = 0; i < tCount; i++)
                    threads[i].join();
                tCount = 0;
                for (int t = 0; t < CPU_COUNT; t++) {
                    pos = bb[t].position();
                    if (pos > 0) {
                        prepareArr(t, pos);
                        bb[t].get(0, buffer[t], 0, pos);
                        bb[t].position(0);
                        tCount++;
                        final int fpos = pos;
                        final int ft = t;
                        threads[t] = new Thread(() -> readBuffer(fpos, ft));
                    }
                }
                for (int i = 0; i < tCount; i++)
                    threads[i].start();
            }

            channel.close();
            file.close();
            Map<String, float[]> full = new HashMap<>(map.get(0));
            for (int t = 1; t < CPU_COUNT; t++) {
                for (String key : map.get(t).keySet()) {
                    float[] val = map.get(t).get(key);
                    if (full.containsKey(key)) {
                        float[] oldVal = full.get(key);
                        full.put(key, new float[]{Math.min(oldVal[0], val[0]), oldVal[1] + val[1], Math.max(oldVal[2], val[2]), oldVal[3] + val[3]});
                    } else {
                        full.put(key, val);
                    }
                }
            }

            PriorityQueue<String> pq = new PriorityQueue<>(Comparator.naturalOrder());
            pq.addAll(full.keySet());

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
            String key;
            float[] ff;
            while ((key = pq.poll()) != null) {
                ff = full.get(key);
                writer.write(key + ' ' + ff[0] + ' ' + ff[1] / ff[3] + ' ' + ff[2] + '\n');
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

    private static void prepareArr(int t, int pos) {
        int i;
        if (t == 0) {
            for (i = 0; i < arr[CPU_COUNT - 1].length; i++) {
                arr[0][i] = arr[CPU_COUNT - 1][i];
                j[0] = j[CPU_COUNT - 1];
                sep[0] = sep[CPU_COUNT - 1];
            }
        } else {
            int p = 0;
            for (i = pos - 1; i > -1; i--) {
                arr[t][p] = (char) buffer[t - 1][i];
                if (buffer[t - 1][i] == 10) {
                    j[t] = p;
                    return;
                }
                if (buffer[t - 1][i] == 59)
                    sep[t] = p;
                p++;
            }
        }
    }

    private static void readBuffer(int pos, int idx) {
        long start = new Date().getTime();
        for (int i = 0; i < pos; i++) {
            arr[idx][j[idx]] = (char) buffer[idx][i];
            if (buffer[idx][i] == 59) {
                sep[idx] = j[idx];
            }
            if (buffer[idx][i] == 10) {
                String key = new String(arr[idx], 0, sep[idx]);
                float f = getFloat(sep[idx], j[idx] - 1, idx);

                if (map.get(idx).containsKey(key)) {
                    float[] ff = map.get(idx).get(key);
                    ff[0] = Math.min(f, ff[0]);
                    ff[1] += f;
                    ff[2] = Math.max(f, ff[2]);
                    ff[3]++;
                    map.get(idx).put(key, ff);
                } else
                    map.get(idx).put(key, new float[]{f, f, f, 1f});

                j[idx] = -1;
            }
            j[idx]++;
        }
        long stop = new Date().getTime();
        fullTime.addAndGet((int) (stop - start));
        callCounter.incrementAndGet();
    }

    private static float getFloat(int from, int to, int idx) {
        float val = 0f;
        int c = 0;
        int d;
        for (int i = to; i > from; i--) {
            d = arr[idx][i] - 48;
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
