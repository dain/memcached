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

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;

public class UnsafeAllocator implements Allocator
{
    public static final Unsafe unsafe;
    public static final BlockCopy blockCopy;
    public static final boolean checkBounds = System.getProperty("org.iq80.memory.CHECK_BOUNDS", "true").equals("true");

    private static final ReferenceQueue<UnsafeAllocation> REFERENCE_QUEUE;

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

        blockCopy = new BlockCopy(unsafe);

        REFERENCE_QUEUE = new ReferenceQueue<UnsafeAllocation>();
        Thread thread = new Thread(new UnsafeMemoryFinalizer(REFERENCE_QUEUE), "UnsafeMemoryFinalizer");
        thread.setDaemon(true);
        // thread.start();      // todo think about automatic collection... should we only use automatic collection?
    }

    public static final int ADDRESS_SIZE = unsafe.addressSize();
    public static final int PAGE_SIZE = unsafe.pageSize();

    public final static UnsafeAllocator INSTANCE = new UnsafeAllocator();

    private UnsafeAllocator() {}

    public Region region(long address, long length)
            throws IndexOutOfBoundsException
    {
        return new UnsafeAllocation(address, length);
    }
    
    public Region region(long address)
            throws IndexOutOfBoundsException
    {
        return new UnsafeAllocation(address, Long.MAX_VALUE-address);
    }

    public Allocation allocate(long size)
            throws IllegalArgumentException, OutOfMemoryError
    {
        if (size < 0) {
            throw new IllegalArgumentException("Size is negative: " + size);
        }

        // Allocating zero bytes, results in a pointer to null
        if (size == 0) {
            return NULL_POINTER;
        }

        long address = unsafe.allocateMemory(size);
        UnsafeAllocation memory = new UnsafeAllocation(address, size);
//        new UnsafeMemoryPhantomReference(memory, referenceQueue); todo
        return memory;
    }

    private static class UnsafeMemoryFinalizer implements Runnable
    {
        private final ReferenceQueue<UnsafeAllocation> referenceQueue;

        private UnsafeMemoryFinalizer(ReferenceQueue<UnsafeAllocation> referenceQueue)
        {
            this.referenceQueue = referenceQueue;
        }

        @SuppressWarnings({"InfiniteLoopStatement"})
        public void run()
        {
            while (true) {
                try {
                    Reference<? extends UnsafeAllocation> reference = referenceQueue.remove();
                    if (reference instanceof UnsafeMemoryPhantomReference) {
                        UnsafeMemoryPhantomReference phantomReference = (UnsafeMemoryPhantomReference) reference;
                        phantomReference.free();
                    }
                }
                catch (InterruptedException ignored) {
                    // this is a daemon thread that can't die
                    Thread.interrupted();
                }
            }
        }
    }

    private static class UnsafeMemoryPhantomReference extends PhantomReference<UnsafeAllocation>
    {
        private final long address;

        public UnsafeMemoryPhantomReference(UnsafeAllocation referent, ReferenceQueue<UnsafeAllocation> q)
        {
            super(referent, q);
            if (referent == null) {
                throw new NullPointerException("referent is null");
            }
            address = referent.address;
            if (address <= 0) {
                throw new IllegalArgumentException("Invalid address " + address);
            }
        }

        public void free()
        {
            unsafe.freeMemory(address);
        }
    }
}
