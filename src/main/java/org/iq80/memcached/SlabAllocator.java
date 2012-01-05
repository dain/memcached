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

import org.iq80.memory.Allocation;
import org.iq80.memory.Allocator;
import org.iq80.memory.Region;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SlabAllocator implements Allocator
{
    /* *
     * Slab sizing definitions.
     */
    static final short POWER_LARGEST = 200;
    private static final int CHUNK_ALIGN_BYTES = 8;

    private final List<SlabManager> slabManagers;
    private final int largestSlabId;
    private final Allocator allocator;

    /**
     * Create a slab allocator.  The available slab sizes are determined and a
     * slab for each size is allocated.
     *
     * @param maxMemory the maximum number of bytes to allocate or 0 for no
     * limit
     * @param factor each slab will use a chunk size equal to the previous
     * slab's chunk size times this factor
     * @param prealloc if true the slab allocator should allocate all memory up
     * front; otherwise memory is allocated in chunks as it is needed
     */
    public SlabAllocator(Allocator allocator, long maxMemory, double factor, boolean prealloc, int chunkSize, int maxItemSize)
    {
        this.allocator = allocator;
        PrivateSlabAllocator privateAllocator = new PrivateSlabAllocator(allocator, maxMemory, prealloc);

        // todo move to caller
        int size = Item.FIXED_SIZE + chunkSize;

        // Factor of 2.0 means use the default memcached behavior
        if (factor == 2.0 && size < 128) {
            size = 128;
        }

        List<SlabManager> slabManagers = new ArrayList<SlabManager>(POWER_LARGEST + 1);
        for (short i = 0; i < POWER_LARGEST && size <= maxItemSize / 2; i++) {
            // Make sure items are always n-byte aligned
            if (size % CHUNK_ALIGN_BYTES != 0) {
                size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
            }

            slabManagers.add(new SlabManager(privateAllocator, (byte) i, size, maxItemSize / size));
            size *= factor;
        }

        // create a manager for the max slab size
        slabManagers.add(new SlabManager(privateAllocator, (byte) slabManagers.size(), maxItemSize, 1));

        this.slabManagers = Collections.unmodifiableList(slabManagers);
        this.largestSlabId = slabManagers.size();
    }

    public int getLargestSlabId()
    {
        return largestSlabId;
    }

    public Allocation allocate(long size)
            throws OutOfMemoryError
    {
        SlabManager slabClass = selectSlabManager(size);
        if (slabClass == null) {
            throw new IllegalArgumentException("Size is greater then largest max size");
        }
        return (Allocation) slabClass.allocate(size);
    }

    @Override
    public Region region(long address, long length) throws IndexOutOfBoundsException {
        return allocator.region(address, length);
    }

    @Override
    public Region region(long address) throws IndexOutOfBoundsException {
        return allocator.region(address);
    }

    public List<SlabManager> getSlabManagers()
    {
        return slabManagers;
    }

    public SlabManager getSlabManager(int id)
    {
        return slabManagers.get(id);
    }

    public SlabManager selectSlabManager(long size)
    {
        for (SlabManager slabClass : slabManagers) {
            if (size < slabClass.getChunkSize()) {
                return slabClass;
            }
        }
        // does not fit in larget size
        return null;
    }
}
