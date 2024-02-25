package com.km;

import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;

public class Main {
    private static InputStream in;

    public static void main(String[] args) {
        try {
            long start = new Date().getTime();
            RandomAccessFile file = new RandomAccessFile("./input.txt", "r");
            FileChannel channel = file.getChannel();
            ByteBuffer bb = ByteBuffer.allocateDirect(1000000);
            int i = 0;
            while (channel.read(bb) > -1) {
                i++;
                bb.position(0);
                System.out.println("read " + i + " position " + channel.position());
            }
            channel.close();
            file.close();
            long stop = new Date().getTime();
            System.out.println("number of lines = " + i);
            System.out.println("execution time = " + (stop - start));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String readString(char c) {
        return null;
    }
}
