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

/**
 * Pointers are NOT thread safe.
 */
public interface Region
{
    Allocator getAllocator();

    ByteBuffer toByteBuffer();

    long getAddress();

    long size();

    byte getByte(long offset);

    void putByte(long offset, byte value);

    byte[] getBytes(long srcOffset, int length);

    void getBytes(long srcOffset, byte[] target);

    void getBytes(long srcOffset, byte[] target, int targetOffset, int length);

    void putBytes(long targetOffset, byte[] src);

    void putBytes(long targetOffset, byte[] src, int srcOffset, int length);

    Region getRegion(long offset);

    Region getRegion(long offset, long length);

    short getShort(long offset);

    void putShort(long offset, short value);

    char getChar(long offset);

    void putChar(long offset, char value);

    int getInt(long offset);

    void putInt(long offset, int value);

    long getLong(long offset);

    void putLong(long offset, long value);

    float getFloat(long offset);

    void putFloat(long offset, float value);

    double getDouble(long offset);

    void putDouble(long offset, double value);

    void setMemory(byte value);

    void setMemory(long offset, long size, byte value);

    void copyMemory(long srcOffset, Region target);

    void copyMemory(long srcOffset, Region target, long targetOffset, long size);

    int compareMemory(long srcOffset, Region target, long targetOffset, long size);

    boolean isInBounds(long offset, long length);

    void checkBounds(long offset, long length);

}
