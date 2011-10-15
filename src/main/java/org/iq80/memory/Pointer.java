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

import static org.iq80.memory.Allocator.DOUBLE_SIZE;
import static org.iq80.memory.Allocator.FLOAT_SIZE;
import static org.iq80.memory.Allocator.INT_SIZE;
import static org.iq80.memory.Allocator.LONG_SIZE;
import static org.iq80.memory.Allocator.SHORT_SIZE;

public class Pointer
{
    private final Region region;
    private long position;

    public Pointer(Region region)
    {
        this.region = region;
    }

    public Pointer(Region region, long position)
    {
        this.region = region;
        this.position = position;
    }

    public boolean hasRemaining()
    {
        return position < region.size();
    }

    public boolean hasRemaining(long size)
    {
        return position + size <= region.size();
    }

    public long getPosition()
    {
        return position;
    }

    public void seek(long length)
    {
        region.checkBounds(position, length);
        position += length;
    }

    public void setPosition(long position)
    {
        region.checkBounds(position, 0);
        this.position = position;
    }

    public byte getByte()
    {
        byte b = region.getByte(position);
        position++;
        return b;
    }

    public void putByte(byte value)
    {
        region.putByte(position, value);
        position++;
    }

    public byte[] getBytes(int length)
    {
        byte[] bytes = region.getBytes(position, length);
        position += length;
        return bytes;
    }

    public void getBytes(byte[] target)
    {
        region.getBytes(position, target);
        position += target.length;
    }

    public void getBytes(byte[] target, int targetOffset, int length)
    {
        region.getBytes(position, target, targetOffset, length);
        position += length;
    }

    public void putBytes(byte[] src)
    {
        region.putBytes(position, src);
        position += src.length;
    }

    public void putBytes(byte[] src, int srcOffset, int length)
    {
        region.putBytes(position, src, srcOffset, length);
        position += length;
    }

    public Region getRegion(long length)
    {
        SubRegion subRegion = new SubRegion(region, position, length);
        position += length;
        return subRegion;
    }

    public short getShort()
    {
        short s = region.getShort(position);
        position += SHORT_SIZE;
        return s;
    }

    public void putShort(short value)
    {
        region.putShort(position, value);
        position += SHORT_SIZE;
    }

    public int getInt()
    {
        int i = region.getInt(position);
        position += INT_SIZE;
        return i;
    }

    public void putInt(int value)
    {
        region.putInt(position, value);
        position += INT_SIZE;
    }

    public long getLong()
    {
        long l = region.getLong(position);
        position += LONG_SIZE;
        return l;
    }

    public void putLong(long value)
    {
        region.putLong(position, value);
        position += LONG_SIZE;
    }

    public float getFloat()
    {
        float i = region.getFloat(position);
        position += FLOAT_SIZE;
        return i;
    }

    public void putFloat(float value)
    {
        region.putFloat(position, value);
        position += FLOAT_SIZE;
    }

    public double getDouble()
    {
        double l = region.getDouble(position);
        position += DOUBLE_SIZE;
        return l;
    }

    public void putDouble(double value)
    {
        region.putDouble(position, value);
        position += DOUBLE_SIZE;
    }

    public void setPointer(byte value)
    {
        long size = region.size() - position;
        if (size < 0) {
            throw new IllegalArgumentException("Size is not set");
        }
        region.setMemory(position, size, value);
        position += size;
    }

    public void setMemory(long size, byte value)
    {
        region.setMemory(position, size, value);
        position += size;
    }

    public void copyMemory(Region target)
    {
        region.copyMemory(position, target);
        position += target.size();
    }

    public void copyMemory(Region target, long targetOffset, long size)
    {
        region.copyMemory(position, target, targetOffset, size);
        position += size;
    }

    public void compareMemory(Region target, long targetOffset, long size)
    {
        region.compareMemory(position, target, targetOffset, size);
        position += size;
    }

    public boolean isInBounds(long length)
    {
        return region.isInBounds(position, length);
    }

    public void checkBounds(long length)
    {
        region.checkBounds(position, length);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("Cursor");
        sb.append("{position=").append(position);
        sb.append(", pointer=").append(region);
        sb.append('}');
        return sb.toString();
    }
}
