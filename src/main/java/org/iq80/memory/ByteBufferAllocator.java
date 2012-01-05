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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class ByteBufferAllocator implements Allocator
{
    public static final ByteBufferAllocator INSTANCE = new ByteBufferAllocator();

    private final AtomicLong nextAddress = new AtomicLong(1);
    private TreeMap allocations = new TreeMap<Long, ByteBufferAllocation>();

    private ByteBufferAllocator() {
    }

    public Region region(long address, long length) throws IndexOutOfBoundsException
    {

        // TODO: is there a concurrent TreeMap we could use?
        // If region lookups are much more frequent than allocations, perhaps
        // we should use a CoW tree map.
        Map.Entry<Long, ByteBufferAllocation> entry = null;
        synchronized (allocations) {
            entry = allocations.floorEntry(address);
        }
        if(entry == null) {
            throw new IndexOutOfBoundsException("Invalid address");
        }
        ByteBufferAllocation allocation=entry.getValue();
        long offset = address - allocation.getAddress();
        return allocation.getRegion(offset, length);
    }

    public Region region(long address) throws IndexOutOfBoundsException
    {

        // TODO: is there a concurrent TreeMap we could use?
        // If region lookups are much more frequent than allocations, perhaps
        // we should use a CoW tree map.
        Map.Entry<Long, ByteBufferAllocation> entry = null;
        synchronized (allocations) {
            entry = allocations.floorEntry(address);
        }
        if(entry == null) {
            throw new IndexOutOfBoundsException("Invalid address");
        }
        ByteBufferAllocation allocation=entry.getValue();
        long offset = address - allocation.getAddress();
        return allocation.getRegion(offset, allocation.size()-offset);
    }

    public Allocation allocate(long size)
    {
        if (size < 0) {
            throw new IllegalArgumentException("Size is negative: " + size);
        }

        if (size > Integer.MAX_VALUE) {
            throw new IllegalAccessError("Size is greater than 31 bits: " + size);
        }

        // Allocating zero bytes, results in a pointer to null
        if (size == 0) {
            return NULL_POINTER;
        }

        long address = nextAddress.getAndAdd(size);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect((int) size);
        ByteBufferAllocation allocation = new ByteBufferAllocation(byteBuffer, address);

        synchronized (allocations) {
            allocations.put(address, allocation);
        }
        return allocation;
    }

    void free(ByteBufferAllocation allocation)
    {
        synchronized (allocations) {
            allocations.remove(allocation.getAddress());
        }
        releaser.release(allocation.toByteBuffer());
    }

    private static interface ByteBufferReleaser {
   		public void release(ByteBuffer buffer);
   	}
    private ByteBufferReleaser releaser = createByteBufferReleaser();
   	private ByteBufferReleaser createByteBufferReleaser() {

   		// Try to drill into the java.nio.DirectBuffer internals...
   		final Method[] cleanerMethods = AccessController.doPrivileged(new PrivilegedAction<Method[]>() {
   			public Method[] run() {
   				try {
   					ByteBuffer buffer = ByteBuffer.allocateDirect(1);
   					Class<?> bufferClazz = buffer.getClass();
   					Method cleanerMethod = bufferClazz.getMethod("cleaner", new Class[0]);
   					cleanerMethod.setAccessible(true);
   					Method cleanMethod = cleanerMethod.getReturnType().getMethod("clean");
   					return new Method[]{cleanerMethod, cleanMethod};
   				} catch (Exception e) {
   					return null;
   				}
   			}
   		});

   		// Yay, we can actually release the buffers.
   		if( cleanerMethods !=null ) {
   			return new ByteBufferReleaser() {
   				public void release(ByteBuffer buffer) {
   					try {
   						Object cleaner = cleanerMethods[0].invoke(buffer);
   						if( cleaner!=null ) {
   							cleanerMethods[1].invoke(cleaner);
   						}
   					} catch (Throwable e) {
   						e.printStackTrace();
   					}
   				}
   			};
   		}

   		// We can't really release the buffers.. Good Luck!
   		return new ByteBufferReleaser() {
   			public void release(ByteBuffer buffer) {
   			}
   		};
   	}

}
