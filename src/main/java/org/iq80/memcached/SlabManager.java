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
package org.iq80.memcached;

import org.iq80.memory.Pointer;
import org.iq80.memory.Region;
import org.iq80.memory.UnsafeAllocation;

import java.util.Arrays;
import java.util.logging.Logger;

public class SlabManager
{
    private static final Logger log = Logger.getLogger(SlabManager.class.getName());

    /**
     * Allocator for slabs
     */
    private final PrivateSlabAllocator allocator;

    /**
     * Id of this slab manager
     */
    private final byte id;

    /**
     * Size of a slot
     */
    private final int chunkSize;

    /**
     * Number of slots that fit into a slab
     */
    private final int chunksPerSlab;

    /**
     * List of free slots
     */
    public long[] freeList = new long[16];

    /**
     * First free slot
     */
    public int freeListCurrsor;

    /**
     * Pointer the next available slot in the open slab.
     */
    public Pointer openSlab;

    /**
     * Number of slabs that have been allocated by this manager
     */
    private int slabCount;

    /**
     * All of the slabs that have been allocated (for debugging)
     */
    private long[] slabs = new long[16];

    /**
     * The number of requested bytes
     */
    public int requested;

    public SlabManager(PrivateSlabAllocator allocator, byte id, int chunkSize, int chunksPerSlab)
    {
        this.allocator = allocator;
        this.id = id;
        this.chunkSize = chunkSize;
        this.chunksPerSlab = chunksPerSlab;

        allocateNewSlab();

        log.info(this.toString());
    }

    public byte getId()
    {
        return id;
    }

    public int getChunkSize()
    {
        return chunkSize;
    }

    public int getChunksPerSlab()
    {
        return chunksPerSlab;
    }

    public int getSlabCount()
    {
        return slabCount;
    }


    public Region allocate(long size)
    {
        // fail unless we have space at the end of a recently allocated page,
        // we have something on our freelist, or we could allocate a new page
        if (openSlab == null && freeListCurrsor != 0) {
            // add a slab
            allocateNewSlab();
        }

        requested += size;

        if (freeListCurrsor != 0) {
            // return off our freelist 
            long address = freeList[--freeListCurrsor];
            return new UnsafeAllocation(address, size);
        }
        else {
            // allocate from the free page
            if (openSlab == null) {
                // add a slab
            }

            Region region = openSlab.getRegion(size);

            // if the open slab is fully committed, clear the reference
            if (openSlab.hasRemaining(size)) {
                openSlab = null;
            }
            return region;
        }
    }

    public void free(Region region, long size)
    {
        if (region == null) {
            throw new NullPointerException("ptr is null");
        }

        // Do we need more slots 
        if (freeListCurrsor == freeList.length) {
            // double slots
            freeList = Arrays.copyOf(freeList, freeList.length * 2);
        }

        freeList[freeListCurrsor++] = region.getAddress();
        requested -= size;
    }

    public void free(long address, long size)
    {
        // Do we need more slots
        if (freeListCurrsor == freeList.length) {
            // double slots
            freeList = Arrays.copyOf(freeList, freeList.length * 2);
        }

        freeList[freeListCurrsor++] = address;
        requested -= size;
    }

    private boolean allocateNewSlab()
    {
        if (openSlab != null) {
            return false;
        }

        // all classes are allowed to allocate a single slab regardless of the limits
        boolean forceAllocation = slabCount > 0;

        Region region = allocator.allocate(chunkSize * chunksPerSlab, forceAllocation);
        if (region == null) {
            throw new OutOfMemoryError("Could not allocate slab");
        }

        region.setMemory((byte) 0);

        openSlab = new Pointer(region);
        log.fine("Allocated slab " + slabCount + " for " + this.toString());

        if (slabs.length == slabCount) {
            slabs = Arrays.copyOf(slabs, slabs.length * 2);
        }
        slabs[slabCount++] = region.getAddress();


        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SlabManager that = (SlabManager) o;

        if (id != that.id) {
            return false;
        }
        if (!allocator.equals(that.allocator)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = allocator.hashCode();
        result = 31 * result + id;
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("SlabManager");
        sb.append("{id=").append(id);
        sb.append(", chunkSize=").append(chunkSize);
        sb.append(", chunksPerSlab=").append(chunksPerSlab);
        sb.append('}');
        return sb.toString();
    }
}
