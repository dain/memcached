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

import org.iq80.memory.Allocator;
import org.iq80.memory.Region;
import org.iq80.memory.UnsafeAllocation;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import static org.iq80.memory.Allocator.BYTE_SIZE;
import static org.iq80.memory.Allocator.INT_SIZE;
import static org.iq80.memory.Allocator.LONG_SIZE;
import static org.iq80.memory.Allocator.SHORT_SIZE;

@SuppressWarnings({"PointlessArithmeticExpression"})
public class Item
{
    private final Logger log = Logger.getLogger(Item.class.getName());

    private static final int ITEM_LINKED = 1;
    private static final int ITEM_CAS = 2;
    // temp
    private static final int ITEM_SLABBED = 4;

    private static final int NEXT_OFFSET = 0;                               //  0
    private static final int PREV_OFFSET = NEXT_OFFSET + LONG_SIZE;         //  8
    private static final int HASH_NEXT_OFFSET = PREV_OFFSET + LONG_SIZE;    // 16
    private static final int TIME_OFFSET = HASH_NEXT_OFFSET + LONG_SIZE;    // 24
    private static final int EXPIRE_TIME_OFFSET = TIME_OFFSET + INT_SIZE;   // 28
    private static final int VALUE_LENGTH_OFFSET = EXPIRE_TIME_OFFSET + INT_SIZE; // 32
    private static final int REF_COUNT_OFFSET = VALUE_LENGTH_OFFSET + INT_SIZE; // 36
    private static final int SUFFIX_LENGTH_OFFSET = REF_COUNT_OFFSET + SHORT_SIZE; // 38
    private static final int FLAGS_OFFSET = SUFFIX_LENGTH_OFFSET + BYTE_SIZE; // 39
    private static final int SLAB_ID_OFFSET = FLAGS_OFFSET + BYTE_SIZE; // 40
    private static final int KEY_LENGTH_OFFSET = SLAB_ID_OFFSET + BYTE_SIZE; // 41

    public static final int FIXED_SIZE = KEY_LENGTH_OFFSET + BYTE_SIZE;

    private static final int CAS_OFFSET = KEY_LENGTH_OFFSET + BYTE_SIZE; // 42   todo should be 8 byte aligned

    private static final int FIXED_SIZE_WITH_CAS = CAS_OFFSET + LONG_SIZE;


    /// **
    //  * Structure for storing items within memcached.
    //  */
    // typedef struct _stritem {
    //     struct _stritem *next;
    //     struct _stritem *prev;
    //     struct _stritem *h_next;    /* hash chain next */
    //     rel_time_t      time;       /* least recent access */
    //     rel_time_t      exptime;    /* expire time */
    //     int             nbytes;     /* size of data */
    //     unsigned short  refcount;
    //     uint8_t         nsuffix;    /* length of flags-and-length string */
    //     uint8_t         it_flags;   /* ITEM_* above */
    //     uint8_t         slabs_clsid;/* which slab class we're in */
    //     uint8_t         nkey;       /* key length, w/terminating null and padding */
    //     void * end[];
    //     /* if it_flags & ITEM_CAS we have 8 bytes CAS */
    //     /* then null-terminated key */
    //     /* then " flags length\r\n" (no terminating null) */
    //     /* then data with terminating \r\n (no terminating null; it's binary!) */
    // } item;

    public static Item cast(long address)
    {
        if (address == 0) {
            return new Item(new UnsafeAllocation(address, FIXED_SIZE));
        }
        return new Item(Allocator.NULL_POINTER);
    }

    public static Item createItem(long totalLength, SlabManager slabManager, boolean useCas)
    {
        Region region = slabManager.allocate(totalLength);
        if (region == null) {
            return null;
        }
        region.setMemory((byte) 0);

        Item item = new Item(region);
        if (useCas) {
            item.setFlags((byte) ITEM_CAS);
        }
        return item;
    }

    public static int calculateTotalSize(int keyLength, int suffixLength, int valueLength, boolean usingCas)
    {
        if (usingCas) {
            return FIXED_SIZE_WITH_CAS + keyLength + 1 + suffixLength + valueLength;
        }
        else {
            return FIXED_SIZE + keyLength + 1 + suffixLength + valueLength;
        }
    }

    private Region region;

    private Item(Region region)
    {
        this.region = region;
    }

    public void init(byte[] key, byte[] suffix, long exptime, int valueLength)
    {

        // verify we have a fresh item
        assert (getSlabId() == 0);

        // reset item
        setNext(0);
        setPrev(0);
        setHashClainNext(0);

        // the caller will have a reference
        setRefCount((short) 1);
        log.fine("REFERENCE: " + this + " *");

        setExptime((int) exptime);

        // key
        setKeyLength((byte) key.length);
        getKey().putBytes(0, key);

        // suffix
        setSuffixLength((byte) suffix.length);
        getSuffix().putBytes(0, suffix);

        // value
        setValueLength(valueLength);
    }

    public void free(SlabManager slabManager)
    {
        // item should not be in the linked list
        assert !isLinked();

        // item should not be referenced
        assert (getRefCount() == 0);

        long totalLength = getTotalSize();

        // Clear the slabId so slab size changer can tell later if item is
        // already free or not
        setSlabId((byte) 0);
        setSlabbed(true);

        log.fine("REFERENCE: " + this + " Free");

        // free item
        slabManager.free(getAddress(), totalLength);
    }

    public void addReference()
    {
        int refCount = getRefCount() + 1;

        // verify count is < short
        setRefCount((short) refCount);

        log.fine("REFERENCE: " + this + " +");
    }

    /**
     * Done using item... may free item
     */
    public void release(SlabAllocator slabAllocator)
    {
        assert !isSlabbed();

        // if ref count is not 0, decrement it
        short refCount = getRefCount();
        if (refCount != 0) {
            refCount--;
            setRefCount(refCount);
            log.fine("REFERENCE: " + this + " -");
        }

        // if ref count is 0 and item has been unlinked, free it
        if (refCount == 0 && !isLinked()) {
            free(slabAllocator.getSlabManager(getSlabId()));
        }
    }

    public void insertAfter(long address)
    {
        assert !isSlabbed();

        // update linked list
        setPrev(0);
        setNext(address);
        if (address != 0) {
            // Cas use doen't matter since use are only using the fixed region of the struct
            cast(address).setPrev(getAddress());
        }
    }

    public void unlink()
    {
        // verify we don't have a circular reference
        assert (getNext() != getAddress());
        assert (getPrev() != getAddress());

        // mark item as unlinked
        setLinked(false);

        // if we have a next, set item.next.prev = item.prev
        if (getNext() != 0) {
            // Cas use doen't matter since use are only using the fixed region of the struct
            cast(getNext()).setPrev(getPrev());
        }
        if (getPrev() != 0) {
            // Cas use doen't matter since use are only using the fixed region of the struct
            cast(getPrev()).setNext(getNext());
        }

    }

    public Region getKey()
    {
        return new UnsafeAllocation(getAddress() + getKeyOffset(), getKeyLength());
    }

    public void setKey(byte[] key)
    {
        setKeyLength((byte) key.length);
        getKey().putBytes(0, key);
    }

    public boolean keyEquals(Region key)
    {
        long keySize = key.size();
        return key.size() == keySize && region.compareMemory(getKeyOffset(), key, 0, keySize) == 0;
    }

    public Region getSuffix()
    {
        return new UnsafeAllocation(getAddress() + getSuffixOffset(), getSuffixLength());
    }

    public Region getValue()
    {
        return new UnsafeAllocation(getAddress() + getValueOffset(), getValueLength());
    }

    public long getAddress()
    {
        return region.getAddress();
    }

    public void setAddress(long address)
    {
        this.region = new UnsafeAllocation(address, FIXED_SIZE);
    }

    /**
     * struct _stritem next: offset=0 length=8
     */
    public long getNext()
    {
        return region.getLong(NEXT_OFFSET);
    }

    public void setNext(long next)
    {
        region.putLong(NEXT_OFFSET, next);
    }

    public void setNext(Item next)
    {
        long nextAddress = next == null ? 0 : next.getAddress();
        region.putLong(NEXT_OFFSET, nextAddress);
    }

    /**
     * struct _stritem prev: offset=8 length=8
     */
    public long getPrev()
    {
        return region.getLong(PREV_OFFSET);
    }

    public void setPrev(long prev)
    {
        region.putLong(PREV_OFFSET, prev);
    }

    public void setPrev(Item prev)
    {
        long prevAddress = prev == null ? 0 : prev.getAddress();
        region.putLong(PREV_OFFSET, prevAddress);
    }

    /**
     * hash chain next struct _stritem h_next: offset=16 length=8
     */
    public long getHashClainNext()
    {
        return region.getLong(HASH_NEXT_OFFSET);
    }

    public void setHashClainNext(long hashClainNext)
    {
        region.putLong(HASH_NEXT_OFFSET, hashClainNext);
    }

    //
    // Time relative to server start. Smaller than time_t on 64-bit systems.
    //

    /**
     * least recent access rel_time_t time: offset=24 length=4
     */
    public int getTime()
    {
        return region.getInt(TIME_OFFSET);
    }

    public void setTime(int time)
    {
        region.putInt(TIME_OFFSET, time);
    }

    /**
     * expire time rel_time_t exptime: offset=28 length=4
     */
    public int getExptime()
    {
        return region.getInt(EXPIRE_TIME_OFFSET);
    }

    public void setExptime(int exptime)
    {
        region.putInt(EXPIRE_TIME_OFFSET, exptime);
    }

    /**
     * size of data int nbytes: offset=32 length=4
     */
    public int getValueLength()
    {
        return region.getInt(VALUE_LENGTH_OFFSET);
    }

    public void setValueLength(int valueLength)
    {
        region.putInt(VALUE_LENGTH_OFFSET, valueLength);
    }

    /**
     * unsigned short refcount: offset=36 length=2
     */
    public short getRefCount()
    {
        return region.getShort(REF_COUNT_OFFSET);
    }

    public void setRefCount(short refCount)
    {
        region.putShort(REF_COUNT_OFFSET, refCount);
    }

    /**
     * length of flags-and-length string uint8_t nsuffix: offset=38 length=1
     */
    public byte getSuffixLength()
    {
        return region.getByte(SUFFIX_LENGTH_OFFSET);
    }

    public void setSuffixLength(byte suffixLength)
    {
        region.putByte(SUFFIX_LENGTH_OFFSET, suffixLength);
    }

    public boolean isLinked()
    {
        return (getFlags() & ITEM_LINKED) != 0;
    }

    public void setLinked(boolean linked)
    {
        byte flags = getFlags();
        if (linked) {
            setFlags((byte) (flags | ITEM_LINKED));
        }
        else {
            setFlags((byte) (getFlags() & ~ITEM_LINKED));
        }
    }

    private boolean isUsingCas()
    {
        return (getFlags() & ITEM_CAS) != 0;
    }

    public boolean isSlabbed()
    {
        return (getFlags() & ITEM_SLABBED) != 0;
    }

    public void setSlabbed(boolean linked)
    {
        byte flags = getFlags();
        if (linked) {
            setFlags((byte) (flags | ITEM_SLABBED));
        }
        else {
            setFlags((byte) (getFlags() & ~ITEM_SLABBED));
        }
    }

    /**
     * uint8_t it_flags: offset=39 length=1
     */
    public byte getFlags()
    {
        return region.getByte(FLAGS_OFFSET);
    }

    public void setFlags(byte flags)
    {
        region.putByte(FLAGS_OFFSET, flags);
    }

    /**
     * Which slab class we're in uint8_t slabs_clsid: offset=40 length=1
     */
    public byte getSlabId()
    {
        // todo mask cast to int and return int
        return region.getByte(SLAB_ID_OFFSET);
    }

    public void setSlabId(byte slabId)
    {
        // todo take int and verify that size < unsigned byte
        region.putByte(SLAB_ID_OFFSET, slabId);
    }

    /**
     * key length, w/terminating null and padding uint8_t nkey: offset=41
     * length=1
     */
    public byte getKeyLength()
    {
        // todo mask cast to int and return int
        return region.getByte(KEY_LENGTH_OFFSET);
    }

    public void setKeyLength(byte keyLength)
    {
        // todo take int and verify that size < unsigned byte
        region.putByte(KEY_LENGTH_OFFSET, keyLength);
    }

    /**
     * todo optional : offset=41 length=8
     */
    public long getCas()
    {
        if (!isUsingCas()) {
            return 0;
        }
        return region.getLong(CAS_OFFSET);
    }

    public void setCas(long cas)
    {
        if (!isUsingCas()) {
            return;
        }
        region.putLong(CAS_OFFSET, cas);
    }

    private int getKeyOffset()
    {
        if (isUsingCas()) {
            return FIXED_SIZE_WITH_CAS;
        }
        else {
            return FIXED_SIZE;
        }
    }

    private int getSuffixOffset()
    {
        if (isUsingCas()) {
            return FIXED_SIZE_WITH_CAS + getKeyLength() + 1;
        }
        else {
            return FIXED_SIZE + getKeyLength() + 1;
        }
    }

    private int getValueOffset()
    {
        if (isUsingCas()) {
            return FIXED_SIZE_WITH_CAS + getKeyLength() + 1 + getSuffixLength();
        }
        else {
            return FIXED_SIZE + getKeyLength() + 1 + getSuffixLength();
        }
    }

    public int getTotalSize()
    {
        return calculateTotalSize(getKeyLength(), getSuffixLength(), getValueLength(), isUsingCas());
    }

    public static class NextChain implements Iterable<Item>
    {
        private final long start;

        public NextChain(long start)
        {
            this.start = start;
        }

        public Iterator<Item> iterator()
        {
            return new Iterator<Item>()
            {
                private Item next = cast(start);

                public boolean hasNext()
                {
                    return next.getAddress() != 0;
                }

                public Item next()
                {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }

                    next.setAddress(next.getNext());
                    return next;
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public static class PrevChain implements Iterable<Item>
    {
        private final long start;

        public PrevChain(long start)
        {
            this.start = start;
        }

        public Iterator<Item> iterator()
        {
            return new Iterator<Item>()
            {
                private Item next = cast(start);

                public boolean hasNext()
                {
                    return next.getAddress() != 0;
                }

                public Item next()
                {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }

                    next.setAddress(next.getPrev());
                    return next;
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public static class HashChain implements Iterable<Item>
    {
        private final long start;

        public HashChain(long start)
        {
            this.start = start;
        }

        public Iterator<Item> iterator()
        {
            return new Iterator<Item>()
            {
                private Item next = cast(start);

                public boolean hasNext()
                {
                    return next.getAddress() != 0;
                }

                public Item next()
                {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }

                    next.setAddress(next.getHashClainNext());
                    return next;
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
