package com.km;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class Main {
    private static final int SIZE = 100000000;
    private static ByteBuffer[] bb;
    private static ByteBuffer one;
    private static final ArrayList<MyMap> map = new ArrayList<>();
    private static byte[][] buffer;
    private static int CPU_COUNT;

    public static void main(String[] args) {
        System.out.println("start");
        MyMap map = new MyMap();
        System.out.println("map created");
        for(int i=0;i<25;i++)
            map.put("id"+i, new int[]{i, 2*i});
        System.out.println("map filled");
        String key;
        while ((key = map.next()) != null) {
            System.out.println("key="+key+" value=["+map.get(key)[0]+","+map.get(key)[1]+"]");
        }
    }

    public static void main2(String[] args) {
        CPU_COUNT = Runtime.getRuntime().availableProcessors();
        bb = new ByteBuffer[CPU_COUNT];
        buffer = new byte[CPU_COUNT][SIZE + 110];
        Thread[] threads = new Thread[CPU_COUNT];
        for (int i = 0; i < CPU_COUNT; i++) {
            bb[i] = ByteBuffer.allocate(SIZE);
            map.add(new MyMap());
        }
        one = ByteBuffer.allocate(1);
        int pos;
        int tCount;
        try {
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
                    final MyMap m = map.get(t);
                    threads[t] = new Thread(() -> readBuffer(fpos, ft, m));
                    threads[t].start();
                }
                for (int i = 0; i < tCount; i++)
                    threads[i].join();
            }

            channel.close();
            file.close();
            MyMap full = map.get(0);
            for (int t = 1; t < CPU_COUNT; t++) {
                map.get(t).resetIterator();
                String key;
                while ((key = map.get(t).next()) != null) {
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
            full.resetIterator();
            String key;
            while ((key = full.next()) != null)
                pq.add(key);

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
            int[] ff;
            while ((key = pq.poll()) != null) {
                ff = full.get(key);
                writer.write(key + ' ' + ((float) ff[0]) / 10 + ' ' + ((float) ff[1]) / ((float) ff[3] * 10) + ' ' + ((float) ff[2]) / 10 + '\n');
            }

            writer.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void readBuffer(int pos, int idx, MyMap m) {
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

    static final class MyMap {
        static int CAP_POW = 14;
        static int MASK = 16383;
        int iterator = 0;
        Node nodeIterator = null;
        Node[] nodes = new Node[1 << CAP_POW];

        boolean containsKey(String key) {
            Node node = nodes[hashToIndex(key.hashCode())];
            if(node == null)
                return false;

            while(node != null) {
                if(node.key.equals(key))
                    return true;
                node = node.next;
            }

            return false;
        }

        int[] get(String key) {
            Node node = nodes[hashToIndex(key.hashCode())];
            do {
                if(node.key.equals(key))
                    return node.value;
                node = node.next;
            } while(node != null);
            return null;
        }

        void put(String key, int[] ff) {
            int idx = hashToIndex(key.hashCode());
            Node node = nodes[idx];
            if(node == null) {
                nodes[idx] = new Node(key, ff);
            } else {
                while(node.next != null)
                    node = node.next;
                node.next = new Node(key, ff);
            }
        }

        void resetIterator() {
            iterator = 0;
            nodeIterator = null;
        }

        String next() {
            if(nodeIterator != null && nodeIterator.next != null) {
                nodeIterator = nodeIterator.next;
                return nodeIterator.key;
            }

            nodeIterator = null;
            while(nodeIterator == null && (iterator < nodes.length - 1)) {
                iterator++;
                nodeIterator = nodes[iterator];
            }
            if(nodeIterator != null)
                return nodeIterator.key;
            return null;
        }

        int hashToIndex(int hash) {
            return hash & MASK;
        }
    }

    static final class Node {
        Node(String k, int[] v) {
            key = k;
            value = v;
        }

        String key;
        int[] value;
        Node next = null;
    }
}
