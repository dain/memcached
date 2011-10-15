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
/* =====================================================================
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 * =====================================================================
 */
package org.iq80.memcached;

import org.iq80.memory.Region;

import java.nio.ByteOrder;

/**
 * Functions for producing 32-bit hashes for hash table lookup.
 */
@SuppressWarnings({"PointlessArithmeticExpression"})
public class Hash
{
    // Why is this so big?  I read 12 bytes at a time into 3 4-byte integers,
    // then mix those integers.  This is fast (you can do a lot more thorough
    // mixing with 12*3 instructions on 3 integers than you can with 3 instructions
    // on 1 byte), but shoehorning those bytes into integers efficiently is messy.


    /**
     * Hash a variable-length key into a 32-bit value.
     * <p/>
     * Every bit of the key affects every bit of the return value.  Two keys
     * differing by one or two bits will have totally different hash values.
     * <p/>
     * The best hash table sizes are powers of 2.  There is no need to do mod a
     * prime (mod is sooo slow!).  If you need less than 32 bits, use a bitMask.
     * For example, if you need only 10 bits, do h = (h & hashmask(10)); In
     * which case, the hash table should have hashSize(10) elements.
     * <p/>
     * If you are hashing n strings (uint8_t **)k, do it like this:
     * <pre>for (i=0, h=0; i&lt;n; ++i) h = hashLittle( k[i], len[i], h);</pre>
     * <p/>
     * By Bob Jenkins, 2006.  bob_jenkins@burtleburtle.net.  You may use this
     * code any way you wish, private, educational, or commercial.  It's free.
     * <p/>
     * Use for hash table lookup, or anything where one collision in 2^^32 is
     * acceptable.  Do NOT use for cryptographic purposes.
     *
     * @param key the key (the unaligned variable-length array of bytes)
     * @param initialValue any int value
     * @returns a 32-bit value.
     */
    public static int hash(Region key, int initialValue)
    {
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
            return hashLittleEndian(key, initialValue);
        }
        else {
            return hashBigEndian(key, initialValue);
        }
    }

    /**
     * Little endian version of the hash function.
     */
    public static int hashLittleEndian(Region key, int initialValue)
    {

        int length = (int) key.size();

        // internal state
        InternalState state = new InternalState(0xdeadbeef + length + initialValue);

        Alignment alignment = Alignment.getAlignment(key.getAddress());
        if (alignment == Alignment.ALIGN_32) {
            //
            // 32 bit aligned
            // As much as possible, read as int
            //
            Region k = key;

            /*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
            while (length > 12) {
                state.a += k.getInt(0 * 4);
                state.b += k.getInt(1 * 4);
                state.c += k.getInt(2 * 4);
                mix(state);
                length -= 12;
                k = k.getRegion(3 * 4);
            }

            //----------------------------- handle the last (probably partial) block

            // "k[2]&0xffffff" actually reads beyond the end of the string, but
            // then masks off the part it's not allowed to read.  Because the
            // string is aligned, the masked-off tail is in the same word as the
            // rest of the string.  Every machine with memory protection I've seen
            // does it on word boundaries, so is OK with this.  But VALGRIND will
            // still catch it and complain.  The masking trick does make the hash
            // noticably faster for short strings (like English words).
            switch (length) {
                // 3 ints
                case 12:
                    state.c += k.getInt(2 * 4);
                    state.b += k.getInt(1 * 4);
                    state.a += k.getInt(0 * 4);
                    break;

                // 0-3 bytes + 2 ints
                case 11:
                    state.c += ((int) k.getByte(10)) << 16;
                    // fall through
                case 10:
                    state.c += ((int) k.getByte(9)) << 8;
                    // fall through
                case 9:
                    state.c += k.getByte(8);
                    // fall through
                case 8:
                    state.b += k.getInt(1 * 4);
                    state.a += k.getInt(0 * 4);
                    break;


                // 0-3 bytes + 1 ints
                case 7:
                    state.b += ((int) k.getByte(6)) << 16;
                    // fall through
                case 6:
                    state.b += ((int) k.getByte(5)) << 8;
                    // fall through
                case 5:
                    state.b += k.getByte(4);
                    // fall through
                case 4:
                    state.a += k.getInt(0 * 4);
                    break;

                // 0-3 bytes + 0 ints
                case 3:
                    state.a += ((int) k.getByte(2)) << 16;
                    // fall through
                case 2:
                    state.a += ((int) k.getByte(1)) << 8;
                    // fall through
                case 1:
                    state.a += k.getByte(0);
                    break;

                // zero length strings require no mixing
                case 0:
                    return state.c;
            }
        }
        else if (alignment == Alignment.ALIGN_16) {
            //
            // 16 bit aligned
            // As much as possible, read as shorts
            //
            Region k = key;

            //--------------- all but last block: aligned reads and different mixing
            while (length > 12) {
                state.a += k.getShort(0 * 2) + (((int) k.getShort(1 * 2)) << 16);
                state.b += k.getShort(2 * 2) + (((int) k.getShort(3 * 2)) << 16);
                state.c += k.getShort(4 * 2) + (((int) k.getShort(5 * 2)) << 16);
                mix(state);
                length -= 12;
                k = k.getRegion(6 * 2);
            }

            //----------------------------- handle the last (probably partial) block
            switch (length) {
                // 6 shorts
                case 12:
                    state.c += k.getShort(4 * 2) + (((int) k.getShort(5 * 2)) << 16);
                    state.b += k.getShort(2 * 2) + (((int) k.getShort(3 * 2)) << 16);
                    state.a += k.getShort(0 * 2) + (((int) k.getShort(1 * 2)) << 16);
                    break;

                // 0-1 bytes + 5 shorts
                case 11:
                    state.c += ((int) k.getByte(10)) << 16;
                    // @fallthrough
                case 10:
                    state.c += k.getShort(2 * 4);
                    state.b += k.getShort(2 * 2) + (((int) k.getShort(2 * 3)) << 16);
                    state.a += k.getShort(2 * 0) + (((int) k.getShort(2 * 1)) << 16);
                    break;

                // 0-1 bytes + 4 shorts
                case 9:
                    state.c += k.getByte(8);
                    // @fallthrough
                case 8:
                    state.b += k.getShort(2 * 2) + (((int) k.getShort(2 * 3)) << 16);
                    state.a += k.getShort(2 * 0) + (((int) k.getShort(2 * 1)) << 16);
                    break;

                // 0-1 bytes + 3 shorts
                case 7:
                    state.b += ((int) k.getByte(6)) << 16;
                    // @fallthrough
                case 6:
                    state.b += k.getShort(2 * 2);
                    state.a += k.getShort(2 * 0) + (((int) k.getShort(2 * 1)) << 16);
                    break;

                // 0-1 bytes + 2 shorts
                case 5:
                    state.b += k.getByte(4);
                    // @fallthrough
                case 4:
                    state.a += k.getShort(2 * 0) + (((int) k.getShort(2 * 1)) << 16);
                    break;

                // 0-1 bytes + 1 shorts
                case 3:
                    state.a += ((int) k.getByte(2)) << 16;
                    // @fallthrough
                case 2:
                    state.a += k.getShort(2 * 0);
                    break;

                // 0-1 bytes + 0 shorts
                case 1:
                    state.a += k.getByte(0);
                    break;

                // zero length strings require no mixing 
                case 0:
                    return state.c;
            }

        }
        else {
            //
            // Not aligned
            // Read one byte at a time
            //
            Region k = key;

            //--------------- all but the last block: affect some 32 bits of (x.a,x.b,x.c)
            while (length > 12) {
                state.a += k.getByte(0);
                state.a += ((int) k.getByte(1)) << 8;
                state.a += ((int) k.getByte(2)) << 16;
                state.a += ((int) k.getByte(3)) << 24;
                state.b += k.getByte(4);
                state.b += ((int) k.getByte(5)) << 8;
                state.b += ((int) k.getByte(6)) << 16;
                state.b += ((int) k.getByte(7)) << 24;
                state.c += k.getByte(8);
                state.c += ((int) k.getByte(9)) << 8;
                state.c += ((int) k.getByte(10)) << 16;
                state.c += ((int) k.getByte(11)) << 24;
                mix(state);
                length -= 12;
                k = k.getRegion(12);
            }

            //-------------------------------- last block: affect all 32 bits of (c)
            switch (length) {
                // all the case statements fall through
                case 12:
                    state.c += ((int) k.getByte(11)) << 24;
                case 11:
                    state.c += ((int) k.getByte(10)) << 16;
                case 10:
                    state.c += ((int) k.getByte(9)) << 8;
                case 9:
                    state.c += k.getByte(8);
                case 8:
                    state.b += ((int) k.getByte(7)) << 24;
                case 7:
                    state.b += ((int) k.getByte(6)) << 16;
                case 6:
                    state.b += ((int) k.getByte(5)) << 8;
                case 5:
                    state.b += k.getByte(4);
                case 4:
                    state.a += ((int) k.getByte(3)) << 24;
                case 3:
                    state.a += ((int) k.getByte(2)) << 16;
                case 2:
                    state.a += ((int) k.getByte(1)) << 8;
                case 1:
                    state.a += k.getByte(0);
                    break;

                // zero length strings require no mixing
                case 0:
                    return state.c;
            }
        }

        finalMix(state);
        return state.c;
    }

    /**
     * hashBigEndian() takes advantage of big-endian byte ordering.
     */
    public static int hashBigEndian(Region key, int initialValue)
    {
        int length = (int) key.size();
        InternalState state = new InternalState(0xdeadbeef + length + initialValue);

        Alignment alignment = Alignment.getAlignment(key.getAddress());
        if (alignment == Alignment.ALIGN_32) {
            //
            // 32 bit aligned
            // As much as possible, read as int
            //
            Region k = key;

            //------ all but last block: aligned reads and affect 32 bits of (a,b,c)
            while (length > 12) {
                state.a += k.getInt(0 * 4);
                state.b += k.getInt(1 * 4);
                state.c += k.getInt(2 * 4);
                mix(state);
                length -= 12;
                k = k.getRegion(3 * 4);
            }

            //----------------------------- handle the last (probably partial) block

            // "k[2]<<8" actually reads beyond the end of the string, but
            // then shifts out the part it's not allowed to read.  Because the
            // string is aligned, the illegal read is in the same word as the
            // rest of the string.  Every machine with memory protection I've seen
            // does it on word boundaries, so is OK with this.  But VALGRIND will
            // still catch it and complain.  The masking trick does make the hash
            // noticably faster for short strings (like English words).
            switch (length) {
                // 3 ints
                case 12:
                    state.c += k.getInt(4 * 2);
                    state.b += k.getInt(4 * 1);
                    state.a += k.getInt(4 * 0);
                    break;

                // 0-3 bytes + 2 ints
                case 11:
                    state.c += ((int) k.getByte(10)) << 8;
                    // fall through
                case 10:
                    state.c += ((int) k.getByte(9)) << 16;
                    // fall through
                case 9:
                    state.c += ((int) k.getByte(8)) << 24;
                    // fall through
                case 8:
                    state.b += k.getInt(4 * 1);
                    state.a += k.getInt(4 * 0);
                    break;

                // 0-3 bytes + 1 ints
                case 7:
                    state.b += ((int) k.getByte(6)) << 8;
                    // fall through
                case 6:
                    state.b += ((int) k.getByte(5)) << 16;
                    // fall through
                case 5:
                    state.b += ((int) k.getByte(4)) << 24;
                    // fall through
                case 4:
                    state.a += k.getInt(4 * 0);
                    break;

                // 0-3 bytes + 0 ints
                case 3:
                    state.a += ((int) k.getByte(2)) << 8;
                    // fall through
                case 2:
                    state.a += ((int) k.getByte(1)) << 16;
                    // fall through
                case 1:
                    state.a += ((int) k.getByte(0)) << 24;
                    break;

                // zero length strings require no mixing
                case 0:
                    return state.c;
            }
        }
        else {
            //
            // Not aligned
            // Read one byte at a time
            //
            Region k = key;

            //--------------- all but the last block: affect some 32 bits of (x.a,x.b,x.c)
            while (length > 12) {
                state.a += ((int) k.getByte(0)) << 24;
                state.a += ((int) k.getByte(1)) << 16;
                state.a += ((int) k.getByte(2)) << 8;
                state.a += ((int) k.getByte(3));
                state.b += ((int) k.getByte(4)) << 24;
                state.b += ((int) k.getByte(5)) << 16;
                state.b += ((int) k.getByte(6)) << 8;
                state.b += ((int) k.getByte(7));
                state.c += ((int) k.getByte(8)) << 24;
                state.c += ((int) k.getByte(9)) << 16;
                state.c += ((int) k.getByte(10)) << 8;
                state.c += ((int) k.getByte(11));
                mix(state);
                length -= 12;
                k = k.getRegion(12);
            }

            //-------------------------------- last block: affect all 32 bits of (x.c)
            switch (length) {
                // all the case statements fall through
                case 12:
                    state.c += k.getByte(11);
                case 11:
                    state.c += ((int) k.getByte(10)) << 8;
                case 10:
                    state.c += ((int) k.getByte(9)) << 16;
                case 9:
                    state.c += ((int) k.getByte(8)) << 24;
                case 8:
                    state.b += k.getByte(7);
                case 7:
                    state.b += ((int) k.getByte(6)) << 8;
                case 6:
                    state.b += ((int) k.getByte(5)) << 16;
                case 5:
                    state.b += ((int) k.getByte(4)) << 24;
                case 4:
                    state.a += k.getByte(3);
                case 3:
                    state.a += ((int) k.getByte(2)) << 8;
                case 2:
                    state.a += ((int) k.getByte(1)) << 16;
                case 1:
                    state.a += ((int) k.getByte(0)) << 24;
                    break;

                // zero length strings require no mixing
                case 0:
                    return state.c;
            }
        }

        finalMix(state);
        return state.c;
    }

    /**
     * Rotates int x by k bit.
     *
     * @param value value to rotate
     * @param bits number of bits to rotate
     * @return the rotated value
     */
    public static int rot(int value, int bits)
    {
        assert bits > 0 && bits < 32 : "Bits must be between 1 and 31 inclusive";
        return (value << bits) ^ (value >>> (32 - bits));
    }

    /**
     * Mixes 3 32-bit values reversibly.
     * <p/>
     * This is reversible, so any information in (a,b,c) before mix() is still
     * in (a,b,c) after mix().
     * <p/>
     * If four pairs of (a,b,c) inputs are run through mix(), or through mix()
     * in reverse, there are at least 32 bits of the output that are sometimes
     * the same for one pair and different for another pair. This was tested
     * for:
     * <p/>
     * <ul> <li>pairs that differed by one bit, by two bits, in any combination
     * of top bits of (a,b,c), or in any combination of bottom bits of
     * (a,b,c).</li> <li>"differ" is defined as +, -, ^, or ~^.  For + and -, I
     * transformed the output delta to a Gray code (a^(a>>1)) so a string of 1's
     * (as is commonly produced by subtraction) look like a single 1-bit
     * difference.</li> <li>the base values were pseudorandom, all zero but one
     * bit set, or all zero plus a counter that starts at zero.</li> <ul>
     * <p/>
     * Some k values for my "a-=c; a^=rot(c,k); c+=b;" arrangement that satisfy
     * this are
     * <pre>
     * 4  6  8 16 19  4
     * 9 15  3 18 27 15
     * 14  9  3  7 17  3
     * </pre>
     * <p/>
     * Well, "9 15 3 18 27 15" didn't quite get 32 bits diffing for "differ"
     * defined as + with a one-bit base and a two-bit delta.  I used
     * http://burtleburtle.net/bob/hash/avalanche.html to choose the operations,
     * constants, and arrangements of the variables.
     * <p/>
     * This does not achieve avalanche.  There are input bits of (a,b,c) that
     * fail to affect some output bits of (a,b,c), especially of a.  The most
     * thoroughly mixed value is c, but it doesn't really even achieve avalanche
     * in c.
     * <p/>
     * This allows some parallelism.  Read-after-writes are good at doubling the
     * number of bits affected, so the goal of mixing pulls in the opposite
     * direction as the goal of parallelism.  I did what I could.  Rotates seem
     * to cost as much as shifts on every machine I could lay my hands on, and
     * rotates are much kinder to the top and bottom bits, so I used rotates.
     */
    public static void mix(InternalState state)
    {
        state.a -= state.c;
        state.a ^= rot(state.c, 4);
        state.c += state.b;

        state.b -= state.a;
        state.b ^= rot(state.a, 6);
        state.a += state.c;

        state.c -= state.b;
        state.c ^= rot(state.b, 8);
        state.b += state.a;

        state.a -= state.c;
        state.a ^= rot(state.c, 16);
        state.c += state.b;

        state.b -= state.a;
        state.b ^= rot(state.a, 19);
        state.a += state.c;

        state.c -= state.b;
        state.c ^= rot(state.b, 4);
        state.b += state.a;
    }

    /**
     * Final mixing of 3 32-bit values (a,b,c) into c
     * <p/>
     * Pairs of (a,b,c) values differing in only a few bits will usually produce
     * values of c that look totally different.  This was tested for
     * <p/>
     * <ul> <li> pairs that differed by one bit, by two bits, in any combination
     * of top bits of (a,b,c), or in any combination of bottom bits of (a,b,c).
     * <li> "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
     * the output delta to a Gray code (a^(a>>1)) so a string of 1's (as is
     * commonly produced by subtraction) look like a single 1-bit difference.
     * <li> the base values were pseudorandom, all zero but one bit set, or all
     * zero plus a counter that starts at zero.</li> <ul>
     * <p/>
     * These constants passed:
     * <pre>
     *  14 11 25 16 4 14 24
     *  12 14 25 16 4 14 24
     * </pre>
     * and these came close:
     * <pre>
     *   4  8 15 26 3 22 24
     *  10  8 15 26 3 22 24
     *  11  8 15 26 3 22 24
     * </pre>
     */
    public static void finalMix(InternalState state)
    {
        state.c ^= state.b;
        state.c -= rot(state.b, 14);
        state.a ^= state.c;
        state.a -= rot(state.c, 11);
        state.b ^= state.a;
        state.b -= rot(state.a, 25);
        state.c ^= state.b;
        state.c -= rot(state.b, 16);
        state.a ^= state.c;
        state.a -= rot(state.c, 4);
        state.b ^= state.a;
        state.b -= rot(state.a, 14);
        state.c ^= state.b;
        state.c -= rot(state.b, 24);
    }

    /**
     * Small struct to pass around internal state of the hash algorithm. In the
     * original C code mix and final mix were written as macros which
     * manipulated 3 ints.  Java does not have macros, and it it not possible to
     * return multiple values from a function, so this struct is used instead.
     */
    public static class InternalState
    {
        public int a;
        public int b;
        public int c;

        public InternalState(int initialValue)
        {
            a = b = c = initialValue;
        }

        public InternalState(int a, int b, int c)
        {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            InternalState that = (InternalState) o;

            if (a != that.a) return false;
            if (b != that.b) return false;
            if (c != that.c) return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = a;
            result = 31 * result + b;
            result = 31 * result + c;
            return result;
        }

        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("[");
            sb.append(String.format("0x%08x", a));
            sb.append(", ");
            sb.append(String.format("0x%08x", b));
            sb.append(", ");
            sb.append(String.format("0x%08x", c));
            sb.append(']');
            return sb.toString();
        }
    }


    private static enum Alignment
    {
        ALIGN_8, ALIGN_16, ALIGN_32;

        public static Alignment getAlignment(long address)
        {
            if ((address & 0x3) == 0) {
                return ALIGN_32;
            }
            if ((address & 0x1) == 0) {
                return ALIGN_16;
            }
            return ALIGN_8;
        }
    }
}
