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
package org.iq80.memcached;

import org.iq80.memory.Allocator;
import org.iq80.memory.Pointer;
import org.iq80.memory.Region;

public class PrivateSlabAllocator
{
    /* Slab sizing definitions. */
    public static final int POWER_SMALLEST = 1;
    public static final int POWER_LARGEST = 200;
    public static final int CHUNK_ALIGN_BYTES = 8;
    public static final boolean DONT_PRE_ALLOCATE_SLABS = false;
    public static final int MAX_NUMBER_OF_SLAB_CLASSES = POWER_LARGEST + 1;


    private final Allocator allocator;
    private final Pointer preAllocatedPointer;

    public PrivateSlabAllocator(Allocator allocator, long maxSize, boolean preallocate)
    {
        this.allocator = allocator;

        Pointer preAllocatedPointer = null;
        if (preallocate) {
            /* Allocate everything in a big chunk with malloc */
            try {
                preAllocatedPointer = new Pointer(allocator.allocate(maxSize), maxSize);
            }
            catch (OutOfMemoryError e) {
                System.err.println("Warning: Failed to allocate requested memory in one large chunk.\nWill allocate in smaller chunks\n");
            }
        }
        this.preAllocatedPointer = preAllocatedPointer;
    }

    public Region allocate(long size, boolean force)
    {
        return memory_allocate(size, force);
    }

    private Region memory_allocate(long size, boolean force)
    {
        if (preAllocatedPointer != null) {
            if (preAllocatedPointer.isInBounds(size)) {
                Region region = preAllocatedPointer.getRegion(size);

                // Align the size for the next allocation
                // this may be off the end of the allocated pre allocated memory which
                // seems weird but we will never allocate from there
                if (size % CHUNK_ALIGN_BYTES != 0) {
                    preAllocatedPointer.seek(CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES));
                }

                return region;
            }

            // if forced allocation, use normal allocator; othersize return null
            if (!force) {
                // no more memory available
                return null;
            }

        }

        // We are not using a preallocated large memory chunk
        try {
            // todo check size
            return allocator.allocate(size);
        }
        catch (OutOfMemoryError outOfMemoryError) {
            // no more memory available
            return null;
        }
    }

    public Region region(long address, long length) throws IndexOutOfBoundsException {
        return allocator.region(address, length);
    }

    public Allocator getAllocator() {
        return allocator;
    }
}
