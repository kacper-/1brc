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
            int[] oldVal;
            for (int t = 1; t < CPU_COUNT; t++) {
                map.get(t).resetIterator();
                String key;
                while ((key = map.get(t).next()) != null) {
                    int[] val = map.get(t).get(key);
                    if ((oldVal = full.get(key)) != null) {
                        oldVal[0] = Math.min(oldVal[0], val[0]);
                        oldVal[1] = oldVal[1] + val[1];
                        oldVal[2] = Math.max(oldVal[2], val[2]);
                        oldVal[3] = oldVal[3] + val[3];
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
                writer.write(key + ' ' + ((float) ff[0]) / 10 + ' ' + ((float) ff[1]) / ((float) ff[3] * 10) + ' ' + ((float) ff[2]) / 10 + ", ");
            }
            writer.write('\n');
            writer.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void readBuffer(int pos, int idx, MyMap m) {
        int ls = 0;
        int sep = 0;
        int[] e;
        for (int i = 0; i < pos; i++) {
            if (buffer[idx][i] == 59) {
                sep = i;
            }
            if (buffer[idx][i] == 10) {
                String key = new String(buffer[idx], ls, sep - ls);
                int f = getInt(buffer[idx][i - 5], buffer[idx][i - 4], buffer[idx][i - 3], buffer[idx][i - 1]);

                if ( (e = m.get(key)) != null) {
                    e[0] = Math.min(f, e[0]);
                    e[1] += f;
                    e[2] = Math.max(f, e[2]);
                    e[3]++;
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
        static int CAP_POW = 17;
        static int MASK = ((int)Math.pow(2, CAP_POW))-1;
        int iterator = 0;
        Node nodeIterator = null;
        Node[] nodes = new Node[1 << CAP_POW];

        int[] get(String key) {
            Node node = nodes[key.hashCode() & MASK];
            while(node != null) {
                if(node.key.equals(key))
                    return node.value;
                node = node.next;
            }
            return null;
        }

        void put(String key, int[] ff) {
            int idx = key.hashCode() & MASK;
            if(nodes[idx] == null) {
                nodes[idx] = new Node(key, ff);
            } else {
                Node node = nodes[idx];
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
    }

    static final class Node {
        Node(String k, int[] v) {
            key = k;
            value = v;
        }

        final String key;
        final int[] value;
        Node next = null;
    }
}
