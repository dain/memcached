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

import static org.iq80.memory.Allocator.CHAR_SIZE;
import static org.iq80.memory.Allocator.DOUBLE_SIZE;
import static org.iq80.memory.Allocator.FLOAT_SIZE;
import static org.iq80.memory.Allocator.INT_SIZE;
import static org.iq80.memory.Allocator.LONG_SIZE;
import static org.iq80.memory.Allocator.SHORT_SIZE;

public class SubRegion implements Region
{
    private final Region delegate;
    private final long delegateOffset;
    private final long size;
    private final boolean checkBounds;

    public SubRegion(Region delegate, long delegateOffset, long size)
    {
        this(delegate, delegateOffset, size, true);
    }

    public SubRegion(Region delegate, long delegateOffset, long size, boolean isCheckBounds)
    {
        if (delegate == null) {
            throw new NullPointerException("pointer is null");
        }
        if (delegateOffset < 0) {
            throw new IllegalArgumentException("Invalid offset: " + delegateOffset);
        }
        if (delegateOffset + size < size) {
            throw new IllegalArgumentException("Address + size is greater than 64 bits");
        }

        delegate.checkBounds(delegateOffset, size);

        this.delegate = delegate;
        this.delegateOffset = delegateOffset;
        this.size = size;
        this.checkBounds = isCheckBounds;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        ByteBuffer bb = delegate.toByteBuffer();
        if( bb == null ) {
            return null;
        }
        bb = bb.duplicate();
        int offset = (int) (bb.position() + delegateOffset);
        bb.position(offset);
        bb.limit((int) (offset+size));
        return bb.slice();
    }

    @Override
    public Allocator getAllocator() {
        return delegate.getAllocator();
    }

    public Region getDelegate()
    {
        return delegate;
    }

    public long getDelegateOffset()
    {
        return delegateOffset;
    }

    public long getAddress()
    {
        return delegate.getAddress() + delegateOffset;
    }

    public long size()
    {
        return size;
    }

    public Region getRegion(long offset)
    {
        checkBounds(offset, 1);
        return new SubRegion(this, offset, size - offset);
    }

    public Region getRegion(long offset, long length)
    {
        checkBounds(offset, length);
        return new SubRegion(this, offset, length);
    }


    public byte getByte(long offset)
    {
        checkBounds(offset, 1);
        long location = delegateOffset + offset;
        return delegate.getByte(location);
    }

    public void putByte(long offset, byte value)
    {
        checkBounds(offset, 1);
        long location = delegateOffset + offset;
        delegate.putByte(location, value);
    }

    public byte[] getBytes(long srcOffset, int length)
    {
        byte[] bytes = new byte[length];
        getBytes(srcOffset, bytes, 0, length);
        return bytes;
    }

    public void getBytes(long srcOffset, byte[] target)
    {
        getBytes(srcOffset, target, 0, target.length);
    }

    public void getBytes(long srcOffset, byte[] target, int targetOffset, int length)
    {
        checkBounds(srcOffset, length);
        if (checkBounds) {
            AllocatorUtil.checkBounds(target.length, targetOffset, length);
        }
        long srcLocation = delegateOffset + srcOffset;
        delegate.getBytes(srcLocation, target, targetOffset, length);
    }

    public void putBytes(long targetOffset, byte[] src)
    {
        putBytes(targetOffset, src, 0, src.length);
    }

    public void putBytes(long targetOffset, byte[] src, int srcOffset, int length)
    {
        checkBounds(targetOffset, length);
        if (checkBounds) {
            AllocatorUtil.checkBounds(src.length, srcOffset, length);
        }
        long targetLocation = delegateOffset + targetOffset;
        delegate.putBytes(targetLocation, src, srcOffset, length);
    }

    public short getShort(long offset)
    {
        checkBounds(offset, SHORT_SIZE);
        long location = delegateOffset + offset;
        return delegate.getShort(location);
    }

    public void putShort(long offset, short value)
    {
        checkBounds(offset, SHORT_SIZE);
        long location = delegateOffset + offset;
        delegate.putShort(location, value);
    }

    public char getChar(long offset)
    {
        checkBounds(offset, CHAR_SIZE);
        long location = delegateOffset + offset;
        return delegate.getChar(location);
    }

    public void putChar(long offset, char value)
    {
        checkBounds(offset, CHAR_SIZE);
        long location = delegateOffset + offset;
        delegate.putChar(location, value);
    }

    public int getInt(long offset)
    {
        checkBounds(offset, INT_SIZE);
        long location = delegateOffset + offset;
        return delegate.getInt(location);
    }

    public void putInt(long offset, int value)
    {
        checkBounds(offset, INT_SIZE);
        long location = delegateOffset + offset;
        delegate.putInt(location, value);
    }

    public long getLong(long offset)
    {
        checkBounds(offset, LONG_SIZE);
        long location = delegateOffset + offset;
        return delegate.getLong(location);
    }

    public void putLong(long offset, long value)
    {
        checkBounds(offset, LONG_SIZE);
        long location = delegateOffset + offset;
        delegate.putLong(location, value);
    }

    public float getFloat(long offset)
    {
        checkBounds(offset, FLOAT_SIZE);
        long location = delegateOffset + offset;
        return delegate.getFloat(location);
    }

    public void putFloat(long offset, float value)
    {
        checkBounds(offset, FLOAT_SIZE);
        long location = delegateOffset + offset;
        delegate.putFloat(location, value);
    }

    public double getDouble(long offset)
    {
        checkBounds(offset, DOUBLE_SIZE);
        long location = delegateOffset + offset;
        return delegate.getDouble(location);
    }

    public void putDouble(long offset, double value)
    {
        checkBounds(offset, DOUBLE_SIZE);
        long location = delegateOffset + offset;
        delegate.putDouble(location, value);
    }

    public void setMemory(byte value)
    {
        delegate.setMemory(delegateOffset, size, value);
    }

    public void setMemory(long offset, long size, byte value)
    {
        checkBounds(offset, size);
        long location = delegateOffset + offset;
        delegate.setMemory(location, size, value);
    }

    public void copyMemory(long srcOffset, Region target)
    {
        copyMemory(srcOffset, target, 0, target.size());
    }

    public void copyMemory(long srcOffset, Region target, long targetOffset, long size)
    {
        checkBounds(srcOffset, size);
        long location = delegateOffset + srcOffset;
        delegate.copyMemory(location, target, targetOffset, size);
    }

    public int compareMemory(long srcOffset, Region target, long targetOffset, long size)
    {
        checkBounds(srcOffset, size);
        return delegate.compareMemory(srcOffset, target, targetOffset, size);
    }

    public boolean isInBounds(long offset, long length)
    {
        if (!AllocatorUtil.isInBounds(size, offset, length)) {
            return false;
        }
        if (delegateOffset + offset < offset) {
            // Address + offset + length is greater than 64 bits
            return false;
        }
        return true;
    }

    public void checkBounds(long offset, long length)
    {
        if (checkBounds) {
            if (delegateOffset + offset < offset) {
                throw new IllegalArgumentException("offset is greater than 64 bits: delegateOffset=" + delegateOffset + ",offset=" + offset);
            }
            delegate.checkBounds(delegateOffset + offset, length);
        }
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

        SubRegion pointer = (SubRegion) o;

        return delegateOffset == pointer.delegateOffset && size == pointer.size;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (delegateOffset ^ (delegateOffset >>> 32));
        result = 31 * result + (int) (size ^ (size >>> 32));
        return result;
    }

    public String toString()
    {
        return "SafePointer{" +
                "delegate=" + delegate +
                ", delegateOffset=" + delegateOffset +
                ", size=" + size +
                (!checkBounds ? ", CHECKS DISABLED" : "") +
                '}';
    }
}
