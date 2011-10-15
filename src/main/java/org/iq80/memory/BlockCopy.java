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

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class BlockCopy
{
    private final Unsafe unsafe;
    private final Class<? extends ByteBuffer> directByteBufferClass;
    private final long addressFieldOffset;
    private final long capacityFieldOffset;
    private final long limitFieldOffset;

    public BlockCopy(Unsafe unsafe)
    {
        this.unsafe = unsafe;
        try {
            directByteBufferClass = getClass().getClassLoader().loadClass("java.nio.DirectByteBuffer").asSubclass(ByteBuffer.class);
            addressFieldOffset = getFieldOffset(unsafe, directByteBufferClass, "address");
            capacityFieldOffset = getFieldOffset(unsafe, directByteBufferClass, "capacity");
            limitFieldOffset = getFieldOffset(unsafe, directByteBufferClass, "limit");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static long getFieldOffset(Unsafe unsafe, Class<?> clazz, String name)
            throws NoSuchFieldException
    {
        while (clazz != Object.class) {
            try {
                Field field = clazz.getDeclaredField(name);
                long offset = unsafe.objectFieldOffset(field);
                return offset;
            }
            catch (Exception ignored) {
            }
            clazz = clazz.getSuperclass();
        }
        throw new RuntimeException("Class " + clazz.getName() + " does not contain a field named " + name);
    }

    public void getBytes(long address, byte[] target, int targetOffset, int length)
    {
        try {
            ByteBuffer byteBuffer = (ByteBuffer) unsafe.allocateInstance(directByteBufferClass);
            unsafe.putLong(byteBuffer, addressFieldOffset, address);
            unsafe.putInt(byteBuffer, capacityFieldOffset, Integer.MAX_VALUE);
            unsafe.putInt(byteBuffer, limitFieldOffset, Integer.MAX_VALUE);

            byteBuffer.get(target, targetOffset, length);
        }
        catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    public void putBytes(long address, byte[] src, int srcOffset, int length)
    {
        try {
            ByteBuffer byteBuffer = (ByteBuffer) unsafe.allocateInstance(directByteBufferClass);
            unsafe.putLong(byteBuffer, addressFieldOffset, address);
            unsafe.putInt(byteBuffer, capacityFieldOffset, Integer.MAX_VALUE);
            unsafe.putInt(byteBuffer, limitFieldOffset, Integer.MAX_VALUE);

            byteBuffer.put(src, srcOffset, length);
        }
        catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }
}
