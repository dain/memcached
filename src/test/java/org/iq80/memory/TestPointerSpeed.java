/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.memory;

import java.nio.ByteBuffer;
import java.util.Random;

public class TestPointerSpeed
{
    private static final byte[] gigaByte = new byte[1024 * 1024 * 1024];
    private static final byte[] megaByte = new byte[1024 * 1024];
    private static final byte[] kiloByte = new byte[1024];
    private static final byte[] buffer = new byte[1024 * 1024 * 1024];

    static {
        new Random().nextBytes(gigaByte);
        new Random().nextBytes(megaByte);
        new Random().nextBytes(kiloByte);
    }

    public static void main(String[] args)
            throws Exception
    {
        TestPointerSpeed speed = new TestPointerSpeed();
        speed.testPointer();
//        speed.testByteArray();
//        speed.testDirectBuffer();
    }

    private void testPointer()
            throws Exception
    {
        long writeGB = 0;
        long writeMB = 0;
        long writeKB = 0;
        long readGB = 0;
        long readMB = 0;
        long readKB = 0;

        UnsafeAllocator allocator = new UnsafeAllocator();
        Allocation pointer = allocator.allocate(gigaByte.length);
        try {
            for (int loops = 0; loops < 20; loops++) {
                long startTime = System.nanoTime();
                try {
                    pointer.putBytes(0, gigaByte);
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        writeGB += elapsedTime;
                    }
                    System.out.println("W GB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    long offset = 0;
                    for (int i = 0; i < 1024; i++) {
                        pointer.putBytes(offset, megaByte);
                        offset += megaByte.length;
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        writeMB += elapsedTime;
                    }
                    System.out.println("W MB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    long offset = 0;
                    for (int i = 0; i < 1024 * 1024; i++) {
                        pointer.putBytes(offset, kiloByte);
                        offset += kiloByte.length;
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        writeKB += elapsedTime;
                    }
                    System.out.println("W KB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    pointer.getBytes(0, gigaByte);
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        readGB += elapsedTime;
                    }
                    System.out.println("R GB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    long offset = 0;
                    for (int i = 0; i < 1024; i++) {
                        pointer.getBytes(offset, megaByte);
                        offset += megaByte.length;
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        readMB += elapsedTime;
                    }
                    System.out.println("R MB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    long offset = 0;
                    for (int i = 0; i < 1024 * 1024; i++) {
                        pointer.getBytes(offset, kiloByte);
                        offset += kiloByte.length;
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        readKB += elapsedTime;
                    }
                    System.out.println("R KB: " + transferRate(elapsedTime, 1024));
                }
            }
        }
        finally {
            pointer.free();
        }

        System.out.flush();
        System.out.println();
        System.out.println("AVG W GB: " + transferRate(writeGB, 1024 * 10.0));
        System.out.println("AVG W MB: " + transferRate(writeMB, 1024 * 10.0));
        System.out.println("AVG W KB: " + transferRate(writeKB, 1024 * 10.0));

        System.out.println("AVG R GB: " + transferRate(readGB, 1024 * 10.0));
        System.out.println("AVG R MB: " + transferRate(readMB, 1024 * 10.0));
        System.out.println("AVG R KB: " + transferRate(readKB, 1024 * 10.0));
        System.out.println();
        System.out.flush();

        System.out.println("Sleeping for 10s before continuing");
        Thread.sleep(10000);
        System.out.println();
        System.out.flush();
    }

    private void testDirectBuffer()
            throws Exception
    {
        long writeGB = 0;
        long writeMB = 0;
        long writeKB = 0;
        long readGB = 0;
        long readMB = 0;
        long readKB = 0;

        ByteBuffer pointer = ByteBuffer.allocateDirect(gigaByte.length);
        try {
            for (int loops = 0; loops < 20; loops++) {
                long startTime = System.nanoTime();
                try {
                    pointer.position(0);
                    pointer.put(gigaByte);
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        writeGB += elapsedTime;
                    }
                    System.out.println("W GB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    pointer.position(0);
                    for (int i = 0; i < 1024; i++) {
                        pointer.put(megaByte);
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        writeMB += elapsedTime;
                    }
                    System.out.println("W MB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    pointer.position(0);
                    for (int i = 0; i < 1024 * 1024; i++) {
                        pointer.put(kiloByte);
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        writeKB += elapsedTime;
                    }
                    System.out.println("W KB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    pointer.position(0);
                    pointer.get(gigaByte);
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        readGB += elapsedTime;
                    }
                    System.out.println("R GB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    pointer.position(0);
                    for (int i = 0; i < 1024; i++) {
                        pointer.get(megaByte);
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        readMB += elapsedTime;
                    }
                    System.out.println("R MB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    pointer.position(0);
                    for (int i = 0; i < 1024 * 1024; i++) {
                        pointer.get(kiloByte);
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        readKB += elapsedTime;
                    }
                    System.out.println("R KB: " + transferRate(elapsedTime, 1024));
                }
            }
        }
        finally {
            pointer = null;
            System.gc();
            System.gc();
        }

        System.out.flush();
        System.out.println();
        System.out.println("AVG W GB: " + transferRate(writeGB, 1024 * 10.0));
        System.out.println("AVG W MB: " + transferRate(writeMB, 1024 * 10.0));
        System.out.println("AVG W KB: " + transferRate(writeKB, 1024 * 10.0));

        System.out.println("AVG R GB: " + transferRate(readGB, 1024 * 10.0));
        System.out.println("AVG R MB: " + transferRate(readMB, 1024 * 10.0));
        System.out.println("AVG R KB: " + transferRate(readKB, 1024 * 10.0));
        System.out.println();
        System.out.flush();

        System.out.println("Sleeping for 10s before continuing");
        Thread.sleep(10000);
        System.out.println();
        System.out.flush();
    }

    private void testByteArray()
            throws Exception
    {
        long writeGB = 0;
        long writeMB = 0;
        long writeKB = 0;
        long readGB = 0;
        long readMB = 0;
        long readKB = 0;

        byte[] pointer = buffer;
        try {
            for (int loops = 0; loops < 20; loops++) {
                long startTime = System.nanoTime();
                try {
                    System.arraycopy(gigaByte, 0, pointer, 0, gigaByte.length);
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        writeGB += elapsedTime;
                    }
                    System.out.println("W GB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    for (int i = 0; i < 1024; i++) {
                        System.arraycopy(megaByte, 0, pointer, megaByte.length * i, megaByte.length);
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        writeMB += elapsedTime;
                    }
                    System.out.println("W MB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    for (int i = 0; i < 1024 * 1024; i++) {
                        System.arraycopy(kiloByte, 0, pointer, kiloByte.length * i, kiloByte.length);
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        writeKB += elapsedTime;
                    }
                    System.out.println("W KB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    System.arraycopy(pointer, 0, gigaByte, 0, gigaByte.length);
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        readGB += elapsedTime;
                    }
                    System.out.println("R GB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    for (int i = 0; i < 1024; i++) {
                        System.arraycopy(pointer, megaByte.length * i, megaByte, 0, megaByte.length);
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        readMB += elapsedTime;
                    }
                    System.out.println("R MB: " + transferRate(elapsedTime, 1024));
                }
                startTime = System.nanoTime();
                try {
                    for (int i = 0; i < 1024 * 1024; i++) {
                        System.arraycopy(pointer, kiloByte.length * i, kiloByte, 0, kiloByte.length);
                    }
                }
                finally {
                    long elapsedTime = System.nanoTime() - startTime;
                    if (loops > 10) {
                        readKB += elapsedTime;
                    }
                    System.out.println("R KB: " + transferRate(elapsedTime, 1024));
                }
            }
        }
        finally {
            pointer = null;
            System.gc();
            System.gc();
        }

        System.out.flush();
        System.out.println();
        System.out.println("AVG W GB: " + transferRate(writeGB, 1024 * 10.0));
        System.out.println("AVG W MB: " + transferRate(writeMB, 1024 * 10.0));
        System.out.println("AVG W KB: " + transferRate(writeKB, 1024 * 10.0));

        System.out.println("AVG R GB: " + transferRate(readGB, 1024 * 10.0));
        System.out.println("AVG R MB: " + transferRate(readMB, 1024 * 10.0));
        System.out.println("AVG R KB: " + transferRate(readKB, 1024 * 10.0));
        System.out.println();
        System.out.flush();

        System.out.println("Sleeping for 10s before continuing");
        Thread.sleep(10000);
        System.out.println();
        System.out.flush();
    }


    private String transferRate(long elapsedTime, double transferSize)
    {

        double timeInSec = elapsedTime / 1000000000.0d;
        double megaBytesPerSec = transferSize / timeInSec;
        return String.format("%5.2f (%5.2f sec)", megaBytesPerSec, timeInSec);
    }
}
