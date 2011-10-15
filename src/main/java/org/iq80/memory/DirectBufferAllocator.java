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
import java.util.concurrent.atomic.AtomicInteger;

public class DirectBufferAllocator implements Allocator
{
    private final AtomicInteger idGenerator = new AtomicInteger();

    public Allocation allocate(long size)
    {
        if (size < 0) {
            throw new IllegalArgumentException("Size is negative: " + size);
        }

        if (size > Integer.MAX_VALUE) {
            throw new IllegalAccessError("Size is greater than 31 bits: " + size);
        }

        // Allocating zero bytes, results in a pointer to null
        if (size == 0) {
            return NULL_POINTER;
        }

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect((int) size);
        return new ByteBufferAllocation(this, byteBuffer, idGenerator.incrementAndGet());
    }
}
