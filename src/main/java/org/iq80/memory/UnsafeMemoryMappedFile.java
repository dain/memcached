/*
 * Copyright 2010 Proofpoint, Inc.
 * Copyright (C) 2012, FuseSource Corp.  All rights reserved.
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

import sun.misc.Unsafe;

import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import static java.nio.channels.FileChannel.MapMode.PRIVATE;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class UnsafeMemoryMappedFile implements Closeable
{
    private static final Unsafe unsafe;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);

            int byteArrayIndexScale = unsafe.arrayIndexScale(byte[].class);
            if (byteArrayIndexScale != 1) {
                throw new IllegalStateException("Byte array index scale must be 1, but is " + byteArrayIndexScale);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final int MAP_RO = 0;
    private static final int MAP_RW = 1;
    private static final int MAP_PV = 2;
    private static final int PAGE_SIZE = unsafe.pageSize();

    /**
     * INTERRUPTED call to truncate
     */
    private static final int INTERRUPTED = -3;

    private final RandomAccessFile randomAccessFile;
    private final FileChannel channel;

    private UnsafeAllocation fullPointer;
    private Region region;
    private final Allocator allocator;

    public UnsafeMemoryMappedFile(Allocator allocator, File file, boolean writable)
            throws IOException, OutOfMemoryError
    {
        this.allocator = allocator;
        randomAccessFile = new RandomAccessFile(file, writable ? "rw" : "r");
        this.channel = randomAccessFile.getChannel();

        int protection;
        if (writable) {
            protection = MAP_RW;
        }
        else {
            protection = MAP_RO;
        }

        // handle zero size directly
        if (channel.size() == 0) {
            // use null pointers
            fullPointer = null;
            region = Allocator.NULL_POINTER;
            return;
        }

        // mmap requires that offset be a multiple of the page size
        // Extend the position to fall on a page boundary
        long alignedPosition = 0;
        long alignedSize = channel.size();

        // map the file
        long alignedAddress;
        try {
            alignedAddress = mmap0(protection, alignedPosition, alignedSize);
        }
        catch (OutOfMemoryError ignored) {
            // run GC and try again
            System.gc();
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException y) {
                Thread.currentThread().interrupt();
            }
            alignedAddress = mmap0(protection, alignedPosition, alignedSize);
        }

        // if address is 0 or negative, mapping failed
        if (alignedAddress <= 0) {
            throw new IOException("Unable to map file");
        }

        // fullPointer is a pointer to the fully mapped space
        // this is used internally for load and unmap
        fullPointer = new UnsafeAllocation(alignedAddress, alignedSize);

        // the user gets a pointer to the unaligned address
        region = new SubRegion(fullPointer, 0, channel.size());
    }

    public UnsafeMemoryMappedFile(Allocator allocator, FileChannel channel, MapMode mode)
            throws IOException, OutOfMemoryError
    {
        this(allocator, channel, mode, 0, channel.size());
    }

    public UnsafeMemoryMappedFile(Allocator allocator, FileChannel channel, MapMode mode, long position, long size)
            throws IOException, OutOfMemoryError
    {
        this.allocator = allocator;
        if (channel == null) {
            throw new IllegalArgumentException("Channel is null");
        }
        if (!channel.isOpen()) {
            throw new IllegalStateException("Channel is closed");
        }
        if (mode == null) {
            throw new IllegalArgumentException("Mode is null");
        }
        if (position < 0) {
            throw new IllegalArgumentException("Position is negative: " + size);
        }
        if (size < 0) {
            throw new IllegalArgumentException("Size is negative: " + size);
        }
        if (position + size < size) {
            throw new IllegalArgumentException("Position + size is greater than 64 bits");
        }

        this.randomAccessFile = null;
        this.channel = channel;

        int protection;
        if (mode == READ_ONLY) {
            protection = MAP_RO;
        }
        else if (mode == READ_WRITE) {
            protection = MAP_RW;
        }
        else if (mode == PRIVATE) {
            protection = MAP_PV;
        }
        else {
            throw new IllegalArgumentException("Unknown mode " + mode);
        }

        // handle zero size directly
        if (size == 0) {
            // use null pointers
            fullPointer = null;
            region = Allocator.NULL_POINTER;
            return;
        }

        // Extend file size if necessary
        if (channel.size() < position + size) {
            while (true) {
                int result = truncate0(size);
                // positive result is a success
                if (result >= 0) {
                    break;
                }
                // the only retryable faulure is an interrupt
                if (result != INTERRUPTED || channel.isOpen()) {
                    throw new IOException("Unable to extend file length to " + size);
                }
            }
        }

        // mmap requires that offset be a multiple of the page size
        // Extend the position to fall on a page boundary
        int pageOffset = (int) (position % PAGE_SIZE);
        long alignedPosition = position - pageOffset;
        long alignedSize = size + pageOffset;

        // map the file
        long alignedAddress;
        try {
            alignedAddress = mmap0(protection, alignedPosition, alignedSize);
        }
        catch (OutOfMemoryError ignored) {
            // run GC and try again
            System.gc();
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException y) {
                Thread.currentThread().interrupt();
            }
            alignedAddress = mmap0(protection, alignedPosition, alignedSize);
        }

        // if address is 0 or negative, mapping failed
        if (alignedAddress <= 0) {
            throw new IOException("Unable to map file");
        }

        // fullPointer is a pointer to the fully mapped space
        // this is used internally for load and unmap
        fullPointer = new UnsafeAllocation(alignedAddress, alignedSize);

        // the user gets a pointer to the unaligned address
        long unalignedAddress = alignedAddress + pageOffset;
        region = new SubRegion(fullPointer, unalignedAddress, size);
    }

    public Region getPointer()
    {
        return region;
    }

    private boolean hasNativeMapping()
    {
        return fullPointer == null || fullPointer.address > 0;
    }

    public boolean isLoaded()
    {
        return hasNativeMapping() || isLoaded0(fullPointer.address, fullPointer.size);
    }

    public void load()
    {
        if (hasNativeMapping()) {
            return;
        }
        load0(fullPointer.address, fullPointer.size, PAGE_SIZE);
    }

    public void force()
    {
        if (hasNativeMapping()) {
            return;
        }
        force0(fullPointer.address, fullPointer.size);
    }

    public void close()
            throws IOException
    {
        try {
            if (!hasNativeMapping()) {
                unmap0(fullPointer.address, fullPointer.size);
            }
        }
        finally {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        }
    }

    private long mmap0(int prot, long position, long length)
            throws IOException
    {
        try {
            Method method = sun.nio.ch.FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class, long.class);
            method.setAccessible(true);
            Long address = (Long) method.invoke(channel, prot, position, length);
            return address == null ? 0 : address;
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    private int unmap0(long address, long length)
    {
        try {
            Method method = sun.nio.ch.FileChannelImpl.class.getDeclaredMethod("unmap0", long.class, long.class);
            method.setAccessible(true);
            Integer result = (Integer) method.invoke(channel, address, length);
            return result == null ? 0 : result;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int truncate0(long size)
    {
        try {
            Field field = sun.nio.ch.FileChannelImpl.class.getDeclaredField("fd");
            field.setAccessible(true);
            FileDescriptor fd = (FileDescriptor) field.get(channel);
            Method method = sun.nio.ch.FileChannelImpl.class.getDeclaredMethod("truncate0", FileDescriptor.class, long.class);
            method.setAccessible(true);
            Integer result = (Integer) method.invoke(channel, fd, size);
            return result == null ? 0 : result;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isLoaded0(long address, long length)
    {
        try {
            Method method = java.nio.MappedByteBuffer.class.getDeclaredMethod("isLoaded0", long.class, long.class);
            method.setAccessible(true);
            Boolean result = (Boolean) method.invoke(null, channel, address, length);
            return result == null ? false : result;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int load0(long address, long length, int pageSize)
    {
        try {
            Method method = java.nio.MappedByteBuffer.class.getDeclaredMethod("load0", long.class, long.class, long.class);
            method.setAccessible(true);
            Integer result = (Integer) method.invoke(null, address, length, pageSize);
            return result == null ? 0 : result;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void force0(long address, long length)
    {
        try {
            Method method = java.nio.MappedByteBuffer.class.getDeclaredMethod("force0", long.class, long.class);
            method.setAccessible(true);
            method.invoke(null, address, length);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
