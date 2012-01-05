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

public class ByteBufferAllocation implements Allocation
{
    private final Allocator allocator;
    private ByteBuffer byteBuffer;
    private final long address;

    public ByteBufferAllocation(Allocator allocator, ByteBuffer byteBuffer, int id)
    {
        if (allocator == null) {
            throw new NullPointerException("allocator is null");
        }
        if (byteBuffer == null) {
            throw new NullPointerException("byteBuffer is null");
        }
        this.allocator = allocator;
        this.byteBuffer = byteBuffer;

        // every byte buffer allocation has a unique int id, so use it for the address
        this.address = ((long) id) << 32;
    }

    public ByteBuffer getBufferSafe()
    {
        ByteBuffer byteBuffer = this.byteBuffer;
        if (byteBuffer == null) {
            throw new IllegalStateException("ByteBufferPointer has already been freed");
        }
        return byteBuffer;
    }

    public void free()
    {
        byteBuffer = null;
    }

    public Allocation reallocate(long size)
    {
        return AllocatorUtil.reallocateWithAllocate(allocator, this, size);
    }

    public long getAddress()
    {
        return address;
    }

    public long size()
    {
        return getBufferSafe().capacity();
    }

    public Region getRegion(long offset)
    {
        checkBounds(offset, 1);
        return new SubRegion(this, offset, size() - offset);
    }

    public Region getRegion(long offset, long length)
    {
        checkBounds(offset, length);
        return new SubRegion(this, offset, length);
    }

    public byte getByte(long index)
    {
        return getBufferSafe().get((int) index);
    }

    public void putByte(long index, byte b)
    {
        getBufferSafe().put((int) index, b);
    }

    public byte[] getBytes(long srcOffset, int length)
    {
        ByteBuffer buffer = getBufferSafe().duplicate();
        buffer.position((int) srcOffset);

        byte[] target = new byte[length];
        buffer.get(target);
        return target;
    }

    public void getBytes(long srcOffset, byte[] target)
    {
        ByteBuffer buffer = getBufferSafe().duplicate();
        buffer.position((int) srcOffset);

        buffer.get(target);
    }

    public void getBytes(long srcOffset, byte[] target, int targetOffset, int length)
    {
        ByteBuffer buffer = getBufferSafe().duplicate();
        buffer.position((int) srcOffset);

        buffer.get(target, targetOffset, length);
    }

    public void putBytes(long targetOffset, byte[] src)
    {
        ByteBuffer buffer = getBufferSafe().duplicate();
        buffer.position((int) targetOffset);

        buffer.put(src);
    }

    public void putBytes(long targetOffset, byte[] src, int srcOffset, int length)
    {
        ByteBuffer buffer = getBufferSafe().duplicate();
        buffer.position((int) targetOffset);

        buffer.put(src, srcOffset, length);
    }

    public char getChar(long index)
    {
        return getBufferSafe().getChar((int) index);
    }

    public void putChar(long index, char value)
    {
        getBufferSafe().putChar((int) index, value);
    }

    public short getShort(long index)
    {
        return getBufferSafe().getShort((int) index);
    }

    public void putShort(long index, short value)
    {
        getBufferSafe().putShort((int) index, value);
    }

    public int getInt(long index)
    {
        return getBufferSafe().getInt((int) index);
    }

    public void putInt(long index, int value)
    {
        getBufferSafe().putInt((int) index, value);
    }

    public long getLong(long index)
    {
        return getBufferSafe().getLong((int) index);
    }

    public void putLong(long index, long value)
    {
        getBufferSafe().putLong((int) index, value);
    }

    public float getFloat(long index)
    {
        return getBufferSafe().getFloat((int) index);
    }

    public void putFloat(long index, float value)
    {
        getBufferSafe().putFloat((int) index, value);
    }

    public double getDouble(long index)
    {
        return getBufferSafe().getDouble((int) index);
    }

    public void putDouble(long index, double value)
    {
        getBufferSafe().putDouble((int) index, value);
    }

    public void setMemory(byte value)
    {
        ByteBuffer bufferSafe = getBufferSafe();

        // kind of lame but it works
        int size = bufferSafe.capacity();
        for (int i = 0; i < size; i++) {
            bufferSafe.put(i, value);
        }
    }

    public void setMemory(long offset, long size, byte value)
    {
        ByteBuffer bufferSafe = getBufferSafe();

        // kind of lame but it works
        int intOffset = (int) offset;
        int intSize = (int) size;
        for (int i = 0; i < intSize; i++) {
            bufferSafe.put(intOffset + i, value);
        }
    }

    public void copyMemory(long srcOffset, Region target)
    {
        copyMemory(srcOffset, target, 0, target.size());
    }

    public void copyMemory(long srcOffset, Region target, long targetOffset, long size)
    {
        if (!(target instanceof ByteBufferAllocation)) {
            AllocatorUtil.copyMemoryByteByByte(this, srcOffset, target, targetOffset, size);
            return;
        }
        ByteBufferAllocation targetMemory = (ByteBufferAllocation) target;

        // limit target
        ByteBuffer targetBuffer = targetMemory.getBufferSafe().duplicate();
        targetBuffer.limit((int) (targetOffset + size));
        targetBuffer.position((int) targetOffset);

        // limit src
        ByteBuffer srcBuffer = getBufferSafe().duplicate();
        srcBuffer.limit((int) (srcOffset + size));
        srcBuffer.position((int) srcOffset);

        targetBuffer.put(srcBuffer);
    }

    public int compareMemory(long srcOffset, Region target, long targetOffset, long size)
    {
        return AllocatorUtil.compareMemory(this, srcOffset, target, targetOffset, size);
    }

    public boolean isInBounds(long offset, long length)
    {
        return AllocatorUtil.isInBounds(getBufferSafe().capacity(), offset, length);
    }

    public void checkBounds(long offset, long length)
    {
        AllocatorUtil.checkBounds(getBufferSafe().capacity(), offset, length);
    }
}
