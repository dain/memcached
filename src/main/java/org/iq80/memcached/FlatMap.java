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

import org.iq80.memcached.FlatMap.Item.HashChain;
import org.iq80.memcached.FlatMap.Item.PrevChain;
import org.iq80.memory.Allocator;
import org.iq80.memory.Region;
import org.iq80.memory.UnsafeAllocation;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static org.iq80.memory.Allocator.BYTE_SIZE;
import static org.iq80.memory.Allocator.INT_SIZE;
import static org.iq80.memory.Allocator.LONG_SIZE;
import static org.iq80.memory.Allocator.SHORT_SIZE;

public class FlatMap
{

    private static final Charset UTF8 = Charset.forName("UTF-8");

    /**
     * Atomic id of the last change to an item, which is used for compare and
     * swap operations.
     */
    private static final AtomicLong CAS_ID = new AtomicLong();

    /**
     * Current time is set once per invocation to reduce
     * System.currentTimeMillis() calls.
     */
    private static int current_time = 0;

    //    private final List<ItemLru> lrus;
    private final List<ItemManager> itemManagers;

    private final SlabAllocator slabAllocator;

    private long oldest_live = 0;
    private boolean useCas = true;

    private final Monitor monitor = null;


    /**
     * How many powers of 2's worth of buckets we use
     */
    private int hashPower;

    /**
     * Main hash table. This is where we look except during expansion.
     */
    private long[] primaryHashtable;

    /**
     * Previous hash table. During expansion, we look here for keys that haven't
     * been moved over to the primary yet.
     */
    private long[] old_hashtable;

    /**
     * Number of items in the hash table.
     */
    private int hash_items = 0;

    /**
     * Are we in the middle of expanding now?
     */
    private boolean expanding;

    /**
     * During expansion we migrate values with bucket granularity; this is how
     * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
     */
    private int expand_bucket = 0;


    private static int hashsize(int n)
    {
        return 1 << n;
    }

    private static int hashmask(int n)
    {
        return hashsize(n) - 1;
    }

    public FlatMap(SlabAllocator slabAllocator)
    {
        this.slabAllocator = slabAllocator;

        this.hashPower = 16;
        primaryHashtable = new long[hashsize(hashPower)];

        List<SlabManager> managerList = slabAllocator.getSlabManagers();

        List<ItemManager> itemManagers = new ArrayList<ItemManager>(managerList.size());
        for (int i = 0; i < managerList.size(); i++) {
            itemManagers.add(new ItemManager(slabAllocator.getSlabManager(i)));
        }
        this.itemManagers = Collections.unmodifiableList(itemManagers);
    }

    /**
     * Find an unused item or creates a new item for the specified data.
     */
    public Item allocateItem(byte[] key, int userFlags, long exptime, int valueLength)
    {
        // the "VALUE" line suffix (flags, size)
        byte[] suffix = String.format(" %d %d\r\n", userFlags, valueLength - 2).getBytes(UTF8);

        // determine total length of the record
        long totalLength = Item.calculateTotalSize(key.length, suffix.length, valueLength, useCas);

        // find the item manager for items of this size
        ItemManager itemManager = null;
        for (ItemManager m : itemManagers) {
            if (totalLength < m.getChunkSize()) {
                itemManager = m;
            }
        }
        if (itemManager == null) {
            return null;
        }

        // find a free item large enough to hold the data
        Item item = itemManager.findFreeItem(totalLength);
        if (item == null) {
            return null;
        }

        // initialize the time with the key and basic data
        item.init(key, suffix, exptime, valueLength);

        return item;
    }

    /**
     * Get with expiriation logic. wrapper around assoc_find which does the lazy
     * expiration logic
     */
    public Item get(Region key)
    {
        Item item = assoc_find(key);
        if (item == null) {
            // todo log "not found in hash"
            return null;
        }

        if (item != null && oldest_live != 0 && oldest_live <= current_time && item.getTime() <= oldest_live) {
            // todo log "nuked by flush"
            // MTSAFE - cache_lock held
            remove(item);
            return null;
        }

        if (item != null && item.getExptime() != 0 && item.getExptime() <= current_time) {
            // todo log "nuked by expire"
            // MTSAFE - cache_lock held
            remove(item);
            return null;
        }

        item.addReference();
        // todo log found

        return item;
    }

    /**
     * Get without expiration logic returns an item whether or not it's
     * expired.
     */
    public Item peek(Region key)
    {
        Item item = assoc_find(key);
        if (item != null) {
            item.addReference();
        }
        return item;
    }

    /**
     * Insert
     */
    public void insert(Item item)
    {
        monitor.itemLink(item);

        itemManagers.get(item.getSlabId()).insert(item);
    }

    /**
     * Delete. Remove and free item.
     */
    public void remove(Item item)
    {
        monitor.itemUnlink(item);

        itemManagers.get(item.getSlabId()).remove(item);
    }

    /**
     * remove old and insert new
     */
    public int replace(Item oldItem, Item newItem)
    {
        monitor.itemReplace(oldItem, newItem);

        assert !oldItem.isSlabbed();

        // todo don't fire monitor events
        itemManagers.get(oldItem.getSlabId()).remove(oldItem);
        itemManagers.get(newItem.getSlabId()).insert(newItem);
        return 1;
    }

    /**
     * Done using item... may free item
     */
    public void release(Item it)
    {
        monitor.itemRemove(it);

        it.release(slabAllocator);
    }

    /**
     * Update item in LRU
     */
    public void touch(Item item)
    {
        itemManagers.get(item.getSlabId()).touch(item);
    }

    /**
     * Walks entire cache, freeing expired items. expires items that are more
     * recent than the oldest_live setting.
     */
    public void flush()
    {
        if (oldest_live == 0) {
            return;
        }

        for (ItemManager itemManager : itemManagers) {
            itemManager.flush();
        }
    }

    private Item assoc_find(Region key)
    {
        int hashCode = Hash.hash(key, 0);

        if (expanding) {
            int oldbucket = hashCode & hashmask(hashPower - 1);
            if (oldbucket >= expand_bucket) {
                return findInBucket(key, old_hashtable, oldbucket);
            }
        }

        return findInBucket(key, primaryHashtable, hashCode & hashmask(hashPower));
    }

    private Item findInBucket(Region key, long[] hashtable, int bucket)
    {
        int depth = 0;
        for (Item item : new HashChain(hashtable[bucket])) {
            if (item.keyEquals(key)) {
                monitor.assocFind(item, depth);
                return item;
            }
            ++depth;
        }

        // log not found  and depth
        return null;
    }

    /**
     * Note: this isn't an assoc_update.  The key must not already exist to call
     * this.
     */
    private void assoc_insert(Item item)
    {

        // shouldn't have duplicately named things defined
        assert assoc_find(item.getKey()) == null;

        int hashCode = Hash.hash(item.getKey(), 0);

        if (expanding) {
            int oldbucket = hashCode & hashmask(hashPower - 1);
            if (oldbucket >= expand_bucket) {
                insertInBucket(item, old_hashtable, oldbucket);
                return;
            }
        }

        insertInBucket(item, primaryHashtable, hashCode & hashmask(hashPower));
    }

    private void insertInBucket(Item item, long[] hashtable, int bucket)
    {
        item.setHashClainNext(hashtable[bucket]);
        hashtable[bucket] = item.getAddress();

        hash_items++;
        if (!expanding && hash_items > (hashsize(hashPower) * 3) / 2) {
            expand();
        }
        monitor.assocInsert(item, hash_items);
    }

    public boolean assoc_delete(Region key)
    {
        //
        // This would be way easier with dummy nodes
        //

        int hashCode = Hash.hash(key, 0);

        if (expanding) {
            int oldbucket = hashCode & hashmask(hashPower - 1);
            if (oldbucket >= expand_bucket) {
                return deleteFromBucket(key, old_hashtable, oldbucket);
            }
            // fallthrough
        }

        return deleteFromBucket(key, primaryHashtable, hashCode & hashmask(hashPower));
    }

    private boolean deleteFromBucket(Region key, long[] hashtable, int bucket)
    {
        // If we didn't find anything, just stop here
        if (hashtable[bucket] != 0) {
            return false;
        }

        Item item = Item.cast(hashtable[bucket]);

        // If this is the one, delete it from the table
        if (item.keyEquals(key)) {
            hash_items--;
            if (item.getHashClainNext() != 0) {
                hashtable[bucket] = item.getHashClainNext();
            }
            item.setHashClainNext(0);
            return true;
        }

        // search though the hash chain...
        Item before = item;
        for (Item next : new HashChain(before.getHashClainNext())) {
            // If this is the one, delete it from the chain
            if (next.keyEquals(key)) {
                hash_items--;
                before.setHashClainNext(next.getHashClainNext());
                next.setHashClainNext(0); // pointless by why not
                return true;
            }
            // remember the previous item in the chain so we can update the hashChainNext pointer
            before.setAddress(next.getAddress());
        }

        // Before should still be non null unless something went wrong
        assert before.getAddress() != 0;

        // Note:  we never actually get here. The callers don't delete things
        // they can't find.

        return false;
    }

    /**
     * Grows the hashtable to the next power of 2.
     */
    private void expand()
    {
        // save off the old hashtable
        old_hashtable = primaryHashtable;

        // create the new primary hashtable
        primaryHashtable = new long[hashsize(hashPower + 1)];

        // log expansion
//        if (settings.verbose > 1) {
//            fprintf(stderr, "Hash table expansion starting\n");
//        }
        hashPower++;
        expanding = true;
        expand_bucket = 0;

        // wake expander thread
//            pthread_cond_signal( & maintenance_cond);
    }


    static volatile boolean do_run_maintenance_thread = true;

    public static final int DEFAULT_HASH_BULK_MOVE = 1;

    int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

    private void assoc_maintenance_thread()
    {

        while (do_run_maintenance_thread) {
            // Lock the cache, and bulk move multiple buckets to the new hash table.
//            pthread_mutex_lock( & cache_lock);

            // migrate a batch of buckets
            for (int i = 0; i < hash_bulk_move && expanding; ++i) {
                for (Item next, item = Item.cast(old_hashtable[expand_bucket]); null != item; item = next) {
                    // remember the next becasue it will be overwritten below
                    next = Item.cast(item.getHashClainNext());

                    // rehash
                    int bucket = Hash.hash(item.getKey(), 0) & hashmask(hashPower);

                    // move item
                    item.setHashClainNext(primaryHashtable[bucket]);
                    primaryHashtable[bucket] = item.getAddress();
                }

                // clear the address in the old table
                old_hashtable[expand_bucket] = 0;

                expand_bucket++;
                if (expand_bucket == hashsize(hashPower - 1)) {
                    expanding = false;
//                    free(old_hashtable);
                    old_hashtable = null;
//                    if (settings.verbose > 1) {
//                        fprintf(stderr, "Hash table expansion done\n");
//                    }
                }
            }

            if (!expanding) {
                /* We are done expanding.. just wait for next invocation */
//                pthread_cond_wait( & maintenance_cond,&cache_lock);
            }

//            pthread_mutex_unlock( & cache_lock);
        }
    }

//    static pthread_t maintenance_tid;

    boolean startAssociationMaintenanceThread()
    {
        int ret;
        String env = System.getProperty("MEMCACHED_HASH_BULK_MOVE");
        if (env != null) {
            try {
                hash_bulk_move = Integer.parseInt(env);
            }
            catch (NumberFormatException ignored) {
            }
            if (hash_bulk_move == 0) {
                hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
            }
        }
        // todo create the thread
//        if ((ret = pthread_create( & maintenance_tid,null,
//            assoc_maintenance_thread, null))!=0){
//        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
//        return -1;
//    }
//        return 0;
        return false;
    }

    void stopAssociationMaintenanceThread()
    {
//        pthread_mutex_lock( & cache_lock);
        do_run_maintenance_thread = false;
//        pthread_cond_signal( & maintenance_cond);
//        pthread_mutex_unlock( & cache_lock);
//
//        /* Wait for the maintenance thread to stop */
//        pthread_join(maintenance_tid, null);
    }


    /**
     * We only reposition items in the LRU queue if they haven't been
     * repositioned in this many seconds. That saves us from churning on
     * frequently-accessed items.
     */
    private static final int ITEM_UPDATE_INTERVAL = 60;

    /**
     * How long an object can reasonably be assumed to be locked before
     * harvesting it on a low memory condition? 3 Hours.
     */
    private static final long TAIL_REPAIR_TIME = 3 * 3600;

    private class ItemManager
    {
        private final SlabManager slabManager;
        private final boolean evictToFree = true;
        private final ItemStats stats = null;
        private final Item head;
        private final Item tail;
        private long size;


        public ItemManager(SlabManager slabManager)
        {
            this.slabManager = slabManager;
            head = Item.createItem(0, slabAllocator.selectSlabManager(Item.FIXED_SIZE), false);
            tail = Item.createItem(0, slabAllocator.selectSlabManager(Item.FIXED_SIZE), false);
        }

        public byte getId()
        {
            return slabManager.getId();
        }

        public int getChunkSize()
        {
            return slabManager.getChunkSize();
        }

        public long getSize()
        {
            return size;
        }

        /**
         * Find a free item slot or create a new item slot.
         */
        public Item findFreeItem(long totalLength)
        {
            // do a quick check if we have any expired items
            Item item = findExpired(50, current_time);
            if (item != null) {
                // remove from hash
                assoc_delete(item.getKey());

                return item;
            }

            // we didn't find a free item, allocate one
            item = Item.createItem(totalLength, slabManager, useCas);
            if (item != null) {
                return item;
            }

            //
            // Could not find an expired item at the tail, and memory allocation
            // failed. Try to evict some items!
            //

            item = freeLeastRecentlyUsed(50, current_time);
            if (item != null) {
                // remove from hash
                assoc_delete(item.getKey());

                return item;
            }

            // try to allocate again
            // maybe someone else freed some stuff
            item = Item.createItem(totalLength, slabManager, useCas);
            if (item != null) {
                return item;
            }

            // Last ditch effort. There is a very rare bug which causes
            // refcount leaks. We've fixed most of them, but it still happens,
            // and it may happen in the future.
            // We can reasonably assume no item can stay locked for more than
            // three hours, so if we find one in the tail which is that old,
            // free it anyway.
            item = tryTailRepair(50, current_time);
            if (item != null) {
                // remove from hash
                assoc_delete(item.getKey());

                return item;
            }

            item = Item.createItem(totalLength, slabManager, useCas);
            return item;
        }

        /**
         * Check if there are any expired items on the tail.
         */
        private Item findExpired(int tries, int currentTime)
        {
            // do a quick check if we have any expired items in the tail..
            for (Item search : new PrevChain(tail)) {
                if (search.getRefCount() == 0 && search.getExptime() != 0 && search.getExptime() < currentTime) {
                    steal(search);

                    return search;
                }

                // only check N entries
                tries--;
                if (tries < 0) {
                    break;
                }
            }
            return null;
        }

        /**
         * Free least recently used.
         */
        private Item freeLeastRecentlyUsed(int tries, int currentTime)
        {
            if (!evictToFree) {
                stats.outOfMemory();
                return null;
            }

//            if (tail == 0) {
//                stats.outOfMemory();
//                return null;
//            }

            for (Item search : new PrevChain(tail)) {
                if (search.getRefCount() == 0) {
                    if (search.getExptime() == 0 || search.getExptime() > currentTime) {
                        stats.evicted(search);
                    }
                    steal(search);
                    return search;
                }

                // only check 50 entries
                tries--;
                if (tries < 0) {
                    break;
                }
            }

            return null;
        }

        /**
         * Search for an item that has been locked for more then 3 hours, which
         * is a bug, and frees that item.
         */
        private Item tryTailRepair(int tries, int currentTime)
        {
            // Last ditch effort. There is a very rare bug which causes
            // refcount leaks. We've fixed most of them, but it still happens,
            // and it may happen in the future.
            // We can reasonably assume no item can stay locked for more than
            // three hours, so if we find one in the tail which is that old,
            // free it anyway.
            for (Item search : new PrevChain(tail)) {
                if (search.getRefCount() != 0 && search.getTime() + TAIL_REPAIR_TIME < currentTime) {
                    stats.tailRepaired(search);
                    steal(search);
                    return search;
                }

                // only check 50 entries
                tries--;
                if (tries < 0) {
                    break;
                }
            }
            return null;
        }

        private void steal(Item item)
        {
            assert item.getRefCount() == 0;

            monitor.itemUnlink(item);

            // if item is still in this map
            if (item.isLinked()) {
                stats.removed(item);

                // remove from LRU
                remove(item);
            }

            // Initialize the item block
            item.setSlabId((byte) 0);
            item.setRefCount((short) 0);
        }

        /**
         * Sets the last modified time of the item to current_time.
         */
        public void touch(Item item)
        {
            // make sure this is the correct manager
            assert item.getSlabId() == slabManager.getId();

            monitor.itemUpdate(item);

            // if item hasn't been moved in 60 seconds, move to head of lru
            if (item.getTime() < current_time - ITEM_UPDATE_INTERVAL) {
                assert !item.isSlabbed();

                if (item.isLinked()) {
                    // rmove item to head of the list
                    item.unlink();
                    head.insertAfter(item);

                    // update the last modified time
                    item.setTime(current_time);

                    // update size
                    size++;
                }
            }
        }

        /**
         * Insert
         */
        public void insert(Item item)
        {
            // make sure this is the correct manager
            assert item.getSlabId() == slabManager.getId();

            // make sure the item is not liked or slabbed
            assert !item.isLinked();
            assert !item.isSlabbed();

//            // make sure the item is new
//            assert item.getAddress() != head;
//            assert item.getAddress() != tail;

            // 1MB max size
            assert item.getValueLength() < 1024 * 1024;

            // mark the item linked
            item.setLinked(true);
            // set the last accessed time
            item.setTime(current_time);
            // add to hash
            assoc_insert(item);

            // Allocate a new CAS ID on link.
            // set the compare and swap id
            if (useCas) {
                item.setCas(CAS_ID.incrementAndGet());
            }

            // add new item after the dummy head of LRU
            head.insertAfter(item);

            // update stats
            size++;
            stats.added(item);
        }

        /**
         * Delete. Remove and free item.
         */
        public void remove(Item item)
        {
            // verify we have a valid item
            assert item.getSlabId() == slabManager.getId();

            // if item is still in this map
            if (item.isLinked()) {
                // remove from hash
                assoc_delete(item.getKey());

                // remove from LRU
                item.unlink();

                // update stats
                size--;
                stats.removed(item);

                // if ref count is 0, free it; otherwise someone is still using it
                if (item.getRefCount() == 0) {
                    item.free(slabAllocator.getSlabManager(item.getSlabId()));
                }
            }
        }

        /**
         * Walks entire cache, freeing expired items. expires items that are
         * more recent than the oldest_live setting.
         */
        public void flush()
        {
            // The LRU is sorted in decreasing time order, and an item's timestamp
            // is never newer than its last access time, so we only need to walk
            // back until we hit an item older than the oldest_live time.
            // The oldest_live checking will auto-expire the remaining items.
            long nextAddress;
            for (Item iter = Item.cast(head.getNext()); iter.getAddress() != tail.getAddress(); iter.setAddress(nextAddress)) {
                if (iter.getTime() < oldest_live) {
                    // We've hit the first old item. Continue to the next queue.
                    break;
                }

                // this is weird but we need to save off the next address and
                // manually move the pointer to avoid a lot of unnecessary o
                // bject creation
                nextAddress = iter.getNext();

                // if the item is not alreayd slabbed, free it
                if (!iter.isSlabbed()) {
                    monitor.itemUnlink(iter);

                    // if item is still in this map
                    if (iter.isLinked()) {
                        stats.removed(iter);

                        // remove from hash
                        assoc_delete(iter.getKey());

                        // remove from LRU
                        remove(iter);

                        // if ref count is 0, free it; otherwise someone is still using it
                        if (iter.getRefCount() == 0) {
                            // head to tail item cannot be freed, unlink first
                            assert iter.getAddress() != head.getAddress();
                            assert iter.getAddress() != tail.getAddress();

                            iter.free(slabManager);
                        }
                    }
                }
            }
        }
    }


    @SuppressWarnings({"PointlessArithmeticExpression"})
    public static class Item
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

        public void insertAfter(Item next)
        {
            assert !isSlabbed();

            // update linked list
            setNext(next);
            if (next != null) {
                next.setPrev(getAddress());
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

            // if we have a prev, set item.prev.next = item.next
            if (getPrev() != 0) {
                // Cas use doen't matter since use are only using the fixed region of the struct
                cast(getPrev()).setNext(getNext());
            }

        }

        public Region getKey()
        {
            return region.getRegion(getKeyOffset(), getKeyLength());
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
            return region.getRegion(getSuffixOffset(), getSuffixLength());
        }

        public Region getValue()
        {
            return region.getRegion(getValueOffset(), getValueLength());
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

            public PrevChain(Item start)
            {
                this.start = start.getAddress();
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

    public interface Monitor
    {
        // MEMCACHED_ITEM_LINK(ITEM_key(it), it.nkey, it.nbytes);
        void itemLink(Item item);

        // MEMCACHED_ITEM_UNLINK(ITEM_key(it), it.nkey, it.nbytes);
        void itemUnlink(Item item);

        // MEMCACHED_ITEM_REMOVE(ITEM_key(it), it.nkey, it.nbytes);
        void itemRemove(Item item);

        // MEMCACHED_ITEM_UPDATE(ITEM_key(it), it.nkey, it.nbytes);
        void itemUpdate(Item item);

        // MEMCACHED_ITEM_REPLACE(ITEM_key(it), it.nkey, it.nbytes,
        //                ITEM_key(new_it), new_it.nkey, new_it.nbytes);
        void itemReplace(Item oldOld, Item newItem);

        // MEMCACHED_ASSOC_FIND(key, nkey, depth);
        void assocFind(Item item, int depth);

        // MEMCACHED_ASSOC_INSERT(ITEM_key(it), it.nkey, hash_items);
        void assocInsert(Item item, int hashItems);
    }

    public interface ItemStats
    {

        void added(Item item);
//        STATS_LOCK();
//        stats.curr_bytes += ITEM_ntotal(it);
//        stats.curr_items += 1;
//        stats.total_items += 1;
//        STATS_UNLOCK();

        void removed(Item item);
//            STATS_LOCK();
//            stats.curr_bytes -= ITEM_ntotal(it);
//            stats.curr_items -= 1;
//            STATS_UNLOCK();

        void evicted(Item item);
//                        itemstats[id].evicted++;
//                        itemstats[id].evicted_time = current_time - search.time;
//                        if (search.exptime != 0) {
//                            itemstats[id].evicted_nonzero++;
//                        }
//                        STATS_LOCK();
//                        stats.evictions++;
//                        STATS_UNLOCK();

        void outOfMemory();
//                itemstats[id].outofmemory++;

        void tailRepaired(Item item);
//                itemstats[id].tailrepairs++;
    }

}
