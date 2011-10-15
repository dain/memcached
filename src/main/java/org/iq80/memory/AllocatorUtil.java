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

import static org.iq80.memory.Allocator.NULL_POINTER;

public final class AllocatorUtil
{
    private AllocatorUtil()
    {
    }

    public static int compareMemory(Region src, long srcOffset, Region target, long targetOffset, long size)
    {
        src.checkBounds(srcOffset, size);
        target.checkBounds(targetOffset, size);

        for (long index = 0; index < size; index++) {
            byte b1 = src.getByte(srcOffset + index);
            byte b2 = target.getByte(targetOffset + index);
            if (b1 > b2) {
                return 1;
            }
            else if (b1 < b2) {
                return -1;
            }
        }
        return 0;
    }

    public static void copyMemoryByteByByte(Region src, long srcOffset, Region target, long targetOffset, long size)
    {
        src.checkBounds(srcOffset, size);
        target.checkBounds(targetOffset, size);

        // todo consider doing blocks of some kind
        // todo make sure that overlapping regions works
        for (long index = 0; index < size; index++) {
            target.putByte(targetOffset + index, src.getByte(srcOffset + index));
        }
    }

    public static Allocation reallocateWithAllocate(Allocator allocator, Allocation allocation, long size)
    {
        if (size < 0) {
            throw new IllegalArgumentException("Size is negative: " + size);
        }

        // If size is unchanged, simply return the same memory instance
        if (size == allocation.size()) {
            return allocation;
        }

        // Allocating zero bytes, results in a memory to null
        if (size == 0) {
            allocation.free();
            return NULL_POINTER;
        }

        // allocate a new memory
        Allocation newAllocation = allocator.allocate(size);
        try {
            // copy the existing data to the new memory
            newAllocation.copyMemory(0, allocation, 0, Math.min(allocation.size(), newAllocation.size()));
        }
        catch (RuntimeException e) {
            // there was a problem copying memory, free the new memory
            newAllocation.free();
            throw e;
        }
        catch (Error e) {
            // there was a problem copying memory, free the new memory
            newAllocation.free();
            throw e;
        }

        // release the old memory
        allocation.free();

        // return the new memory
        return newAllocation;

    }

    public static boolean isInBounds(long size, long offset, long length)
    {
        if (offset < 0) {
            // Offset is negative
            return false;
        }
        if (length < 0) {
            // Length is negative
            return false;
        }
        if (offset + length < length) {
            // Offset + length is greater than 64 bits
            return false;
        }
        if (offset + length > size) {
            // Offset + length is greater than size
            return false;
        }
        return true;
    }

    public static void checkBounds(long size, long offset, long length)
    {
        if (offset < 0) {
            throw new IndexOutOfBoundsException("Offset is negative " + offset);
        }
        if (length < 0) {
            throw new IndexOutOfBoundsException("Length is negative " + length);
        }
        if (offset + length < length) {
            throw new IllegalArgumentException("Offset + length is greater than 64 bits: offset=" + offset + ", length=" + length);
        }
        if (offset + length > size) {
            throw new IndexOutOfBoundsException("Offset + length is greater than size: offset=" + offset + ", length=" + length + ", size=" + size);
        }
    }
}
