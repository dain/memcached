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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


@Test
public class PointerTest
{
    private static final int BYTE_BYTES = 1;
    private static final int SHORT_BYTES = 2;
    private static final int CHAR_BYTES = 2;
    private static final int INT_BYTES = 4;
    private static final int LONG_BYTES = 8;
    private static final int FLOAT_BYTES = 4;
    private static final int DOUBLE_BYTES = 8;
    private UnsafeAllocator unsafeAllocator;

    @BeforeMethod
    protected void setUp()
            throws Exception
    {
        unsafeAllocator = new UnsafeAllocator();
    }

    public void testBasics()
    {
        UnsafeAllocation pointer = new UnsafeAllocation(33, 2000, true);
        assertEquals(pointer.address, 33);
        assertEquals(pointer.size, 2000);

        // Address and size should be used for equals, hashCode and comparison
        assertEquals(pointer, new UnsafeAllocation(33, 2000));
        assertEquals(pointer, new UnsafeAllocation(33, 2000, false));
        assertTrue(!pointer.equals(new UnsafeAllocation(33, 4000)));
        assertTrue(!pointer.equals(new UnsafeAllocation(34, 2000)));

        assertEquals(pointer.hashCode(), new UnsafeAllocation(33, 2000).hashCode());
        assertEquals(pointer.hashCode(), new UnsafeAllocation(33, 2000, false).hashCode());
        assertTrue(pointer.hashCode() != new UnsafeAllocation(33, 4000).hashCode());
        assertTrue(pointer.hashCode() != new UnsafeAllocation(34, 2000).hashCode());

        // pointer with negative address
        try {
            new UnsafeAllocation(-1, 10);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }

        // pointer with negative size
        try {
            new UnsafeAllocation(10, -1);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }


        // malloc negative size
        try {
            unsafeAllocator.allocate(-1);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }

        // malloc too much memory
        // Note: This can not be tested. Once malloc fails, allocation is totally
        // disabled and the VM must be restarted
        // try {
        //    malloc(Long.MAX_VALUE);
        //    fail("Expected OutOfMemoryError");
        // } catch (OutOfMemoryError expected) {
        // }
    }

    public void testNullUnsafeMemory()
    {
        // malloc no memory
        Allocation nullUnsafeAllocation = unsafeAllocator.allocate(0);
//        assertEquals(nullUnsafeMemory.address, 0);
//        assertEquals(nullUnsafeMemory.size, 0);
//        assertSame(nullUnsafeMemory, NULL_POINTER);
//
//        // free should work but have no effect
//        nullUnsafeMemory.free();
//
//        // realloc results in a malloc and a new address
//        Memory newUnsafeMemory = nullUnsafeMemory.reallocate(21);
//        try {
//            assertNotSame(newUnsafeMemory, nullUnsafeMemory);
//            assertEquals(newUnsafeMemory.size, 21);
//            assertTrue(newUnsafeMemory.address > 0);
//        } finally {
//            newUnsafeMemory.free();
//        }

        // no operations should be invokable
        // byte
        try {
            nullUnsafeAllocation.getByte(0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.putByte(0, (byte) 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }

        // short
        try {
            nullUnsafeAllocation.getShort(0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.putShort(0, (short) 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }

        // char
        try {
            nullUnsafeAllocation.getChar(0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.putChar(0, (char) 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        // int
        try {
            nullUnsafeAllocation.getInt(0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.putInt(0, 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        // long
        try {
            nullUnsafeAllocation.getLong(0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.putLong(0, 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        // float
        try {
            nullUnsafeAllocation.getFloat(0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.putFloat(0, 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        // double
        try {
            nullUnsafeAllocation.getDouble(0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.putDouble(0, 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }

        // Byte array
        // Full array size
        byte[] bytes = new byte[0];
        try {
            nullUnsafeAllocation.getBytes(0, 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.getBytes(0, bytes);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.getBytes(0, bytes, 0, 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.putBytes(0, bytes);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.putBytes(0, bytes, 0, 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        // setMemory
        try {
            nullUnsafeAllocation.setMemory((byte) 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
        try {
            nullUnsafeAllocation.setMemory(0, 0, (byte) 0);
            fail("Expected IllegalStateException");
        }
        catch (UnsupportedOperationException expected) {
        }
    }

    public void testSetMemory()
    {
        Allocation pointer = unsafeAllocator.allocate(100);
        try {
            pointer.setMemory((byte) 99);
            for (int i = 0; i < pointer.size(); i++) {
                assertEquals(pointer.getByte(0), 99);
            }

            pointer.setMemory(10, 80, (byte) 11);
            for (int i = 0; i < pointer.size(); i++) {
                if (i < 10) {
                    assertEquals(pointer.getByte(i), 99);
                }
                else if (i < 90) {
                    assertEquals(pointer.getByte(i), 11, "" + i);
                }
                else {
                    assertEquals(pointer.getByte(i), 99);
                }
            }
        }
        finally {
            pointer.free();
        }
    }

    public void testPrimitiveAccess()
    {
        Allocation pointer = unsafeAllocator.allocate(100);
        try {
            // byte
            pointer.setMemory((byte) 0);
            assertEquals(pointer.getByte(0), 0);
            pointer.putByte(0, (byte) 42);
            assertEquals(pointer.getByte(0), 42);

            // short
            pointer.setMemory((byte) 0);
            assertEquals(pointer.getShort(0), 0);
            pointer.putShort(0, (short) 42);
            assertEquals(pointer.getShort(0), 42);

            // char
            pointer.setMemory((byte) 0);
            assertEquals(pointer.getChar(0), 0);
            pointer.putChar(0, (char) 42);
            assertEquals(pointer.getChar(0), 42);

            // int
            pointer.setMemory((byte) 0);
            assertEquals(pointer.getInt(0), 0);
            pointer.putInt(0, 42);
            assertEquals(pointer.getInt(0), 42);

            // long
            pointer.setMemory((byte) 0);
            assertEquals(pointer.getLong(0), 0);
            pointer.putLong(0, 42);
            assertEquals(pointer.getLong(0), 42);

            // float
            pointer.setMemory((byte) 0);
            assertEquals(pointer.getFloat(0), 0.0f, 0.0001f);
            pointer.putFloat(0, 42);
            assertEquals(pointer.getFloat(0), 42.0f, 0.0001f);

            // double
            pointer.setMemory((byte) 0);
            assertEquals(pointer.getDouble(0), 0, 0.0001d);
            pointer.putDouble(0, 42);
            assertEquals(pointer.getDouble(0), 42, 0.0001d);

        }
        finally {
            pointer.free();
        }
    }

    public void testByteArrayAccess()
    {
        Allocation pointer = unsafeAllocator.allocate(100);
        try {
            // getBytes
            for (int i = 0; i < pointer.size(); i++) {
                pointer.putByte(i, (byte) i);
            }
            byte[] bytes = pointer.getBytes(0, (int) pointer.size());
            for (int i = 0; i < bytes.length; i++) {
                assertEquals(bytes[i], i);
            }

            // getBytes
            for (int i = 0; i < pointer.size(); i++) {
                pointer.putByte(i, (byte) -i);
            }
            pointer.getBytes(0, bytes);
            for (int i = 0; i < bytes.length; i++) {
                assertEquals(bytes[i], (byte) -i);
            }

            // putBytes
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) (42 + i);
            }
            pointer.putBytes(0, bytes);
            for (int i = 0; i < pointer.size(); i++) {
                assertEquals(pointer.getByte(i), (byte) (42 + i));
            }
        }
        finally {
            pointer.free();
        }
    }

    public void testBoundsChecking()
    {
        Allocation pointer = unsafeAllocator.allocate(100);
        try {
            pointer.setMemory((byte) 0);

            // byte
            for (int i = BYTE_BYTES; i >= -1; i--) {
                try {
                    pointer.getByte(pointer.size() - i);
                    if (i != BYTE_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == BYTE_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
                try {
                    pointer.putByte(pointer.size() - i, (byte) 0);
                    if (i != BYTE_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == BYTE_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
            }

            // short
            for (int i = SHORT_BYTES; i >= -1; i--) {
                try {
                    pointer.getShort(pointer.size() - i);
                    if (i != SHORT_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == SHORT_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
                try {
                    pointer.putShort(pointer.size() - i, (short) 0);
                    if (i != SHORT_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == SHORT_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
            }

            // char
            for (int i = CHAR_BYTES; i >= -1; i--) {
                try {
                    pointer.getChar(pointer.size() - i);
                    if (i != CHAR_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == CHAR_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
                try {
                    pointer.putChar(pointer.size() - i, '\0');
                    if (i != CHAR_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == CHAR_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
            }

            // int
            for (int i = INT_BYTES; i >= -1; i--) {
                try {
                    pointer.getInt(pointer.size() - i);
                    if (i != INT_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == INT_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
                try {
                    pointer.putInt(pointer.size() - i, 0);
                    if (i != INT_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == INT_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
            }

            // long
            for (int i = LONG_BYTES; i >= -1; i--) {
                try {
                    pointer.getLong(pointer.size() - i);
                    if (i != LONG_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == LONG_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
                try {
                    pointer.putLong(pointer.size() - i, 0);
                    if (i != LONG_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == LONG_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
            }

            // float
            for (int i = FLOAT_BYTES; i >= -1; i--) {
                try {
                    pointer.getFloat(pointer.size() - i);
                    if (i != FLOAT_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == FLOAT_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
                try {
                    pointer.putFloat(pointer.size() - i, 0);
                    if (i != FLOAT_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == FLOAT_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
            }

            // double
            for (int i = DOUBLE_BYTES; i >= -1; i--) {
                try {
                    pointer.getDouble(pointer.size() - i);
                    if (i != DOUBLE_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == DOUBLE_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
                try {
                    pointer.putDouble(pointer.size() - i, 0);
                    if (i != DOUBLE_BYTES) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == DOUBLE_BYTES) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
            }

            // Byte array
            // Full array size
            byte[] bytes = new byte[17];
            for (int i = bytes.length; i >= -1; i--) {
                // getBytes(length)
                try {
                    pointer.getBytes(pointer.size() - i, bytes.length);
                    if (i != bytes.length) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == bytes.length) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }

                // getBytes(byte[])
                try {
                    pointer.getBytes(pointer.size() - i, bytes);
                    if (i != bytes.length) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == bytes.length) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }

                // getBytes(byte[], offset, length)
                try {
                    pointer.getBytes(pointer.size() - i, bytes, 0, bytes.length);
                    if (i != bytes.length) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == bytes.length) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }

                // putBytes(byte[])
                try {
                    pointer.putBytes(pointer.size() - i, bytes);
                    if (i != bytes.length) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == bytes.length) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }

                // putBytes(byte[])
                try {
                    pointer.putBytes(pointer.size() - i, bytes, 0, bytes.length);
                    if (i != bytes.length) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == bytes.length) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
            }
            // Partial array size
            int partialSize = 5;
            for (int i = partialSize; i >= -1; i--) {
                // getBytes(byte[], offset, length)
                try {
                    pointer.getBytes(pointer.size() - i, bytes, 0, partialSize);
                    if (i != partialSize) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == partialSize) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }

                // putBytes(byte[])
                try {
                    pointer.putBytes(pointer.size() - i, bytes, 0, partialSize);
                    if (i != partialSize) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == partialSize) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
            }
            try {
                pointer.getBytes(0, bytes, -1, bytes.length);
                fail("Expected IndexOutOfBoundsException");
            }
            catch (IndexOutOfBoundsException expected) {
            }
            try {
                pointer.getBytes(0, bytes, 0, bytes.length + 1);
                fail("Expected IndexOutOfBoundsException");
            }
            catch (IndexOutOfBoundsException expected) {
            }
            try {
                pointer.putBytes(0, bytes, -1, bytes.length);
                fail("Expected IndexOutOfBoundsException");
            }
            catch (IndexOutOfBoundsException expected) {
            }
            try {
                pointer.putBytes(0, bytes, 0, bytes.length + 1);
                fail("Expected IndexOutOfBoundsException");
            }
            catch (IndexOutOfBoundsException expected) {
            }

            // setMemory
            for (int i = partialSize; i >= -1; i--) {
                // getBytes(byte[], offset, length)
                try {
                    pointer.setMemory(pointer.size() - i, partialSize, (byte) 0);
                    if (i != partialSize) {
                        fail("Expected IndexOutOfBoundsException " + i);
                    }
                }
                catch (IndexOutOfBoundsException expected) {
                    if (i == partialSize) {
                        fail("Unxpected IndexOutOfBoundsException " + i);
                    }
                }
            }

        }
        finally {
            pointer.free();
        }
    }

    public void testRealloc()
    {
        UnsafeAllocation pointer = (UnsafeAllocation) unsafeAllocator.allocate(2000);
        try {
            pointer.putLong(0, 1234567890L);

            // realloc to smaller size should result in the same address
            UnsafeAllocation oldUnsafeMemory = pointer;
            pointer = (UnsafeAllocation) pointer.reallocate(100);
            // todo Java 6 seems to always give a new address
            // assertEquals(pointer.address, oldUnsafeMemory.address);
            // assertNotSame(pointer, oldUnsafeMemory);
            assertEquals(pointer.getLong(0), 1234567890L);

            // realloc to larger size which could result in a new address, but not necessarily
            oldUnsafeMemory = pointer;
            pointer = (UnsafeAllocation) pointer.reallocate(50000);
            assertNotSame(pointer, oldUnsafeMemory);
            assertEquals(pointer.getLong(0), 1234567890L);
        }
        finally {
            pointer.free();
        }
    }

    public void testCopyMemory()
    {
        UnsafeAllocation pointer = (UnsafeAllocation) unsafeAllocator.allocate(2000);
        UnsafeAllocation otherUnsafeMemory = null;
        try {
            for (int i = 0; i < pointer.size; i++) {
                pointer.putByte(i, (byte) i);
            }

            // full copy
            otherUnsafeMemory = (UnsafeAllocation) unsafeAllocator.allocate(2000);
            pointer.copyMemory(0, otherUnsafeMemory);
            for (int i = 0; i < otherUnsafeMemory.size; i++) {
                assertEquals(otherUnsafeMemory.getByte(i), (byte) i);
            }
            pointer.copyMemory(0, otherUnsafeMemory.address, otherUnsafeMemory.size);
            for (int i = 0; i < otherUnsafeMemory.size; i++) {
                assertEquals(otherUnsafeMemory.getByte(i), (byte) i);
            }

            // partial copy
            int partialOffset = 55;
            int partialSize = 500;
            otherUnsafeMemory.setMemory((byte) 0);
            try {
                pointer.copyMemory(33, otherUnsafeMemory, partialOffset, partialSize);
                for (int i = 0; i < partialSize; i++) {
                    assertEquals(otherUnsafeMemory.getByte(partialOffset + i), (byte) (i + 33));
                }
            }
            finally {
                otherUnsafeMemory.free();
            }

            // partial copy
            otherUnsafeMemory = (UnsafeAllocation) unsafeAllocator.allocate(500);
            otherUnsafeMemory.setMemory((byte) 0);
            pointer.copyMemory(33, otherUnsafeMemory);
            for (int i = 0; i < otherUnsafeMemory.size; i++) {
                assertEquals(otherUnsafeMemory.getByte(i), (byte) (i + 33));
            }
        }
        finally {
            pointer.free();
            if (otherUnsafeMemory != null) {
                otherUnsafeMemory.free();
            }
        }
    }
}
