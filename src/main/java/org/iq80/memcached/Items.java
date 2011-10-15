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

import org.iq80.memory.Region;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Items
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

    private final List<ItemLru> lrus;

    private final SlabAllocator slabAllocator;

    private long oldest_live = 0;
    private boolean useCas = true;

    // Hash of items
    private final Association assoc;

    private final Monitor monitor = null;

    public Items(SlabAllocator slabAllocator)
    {
        this.slabAllocator = slabAllocator;
        this.assoc = new Association(monitor);

        List<SlabManager> managerList = slabAllocator.getSlabManagers();

        List<ItemLru> lrus = new ArrayList<ItemLru>(managerList.size());
        for (int i = 0; i < managerList.size(); i++) {
            lrus.add(new ItemLru(slabAllocator.getSlabManager(i), true, monitor, null));
        }
        this.lrus = Collections.unmodifiableList(lrus);
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

        // find a free item large enough to hold the data
        Item item = findFreeItem(totalLength);
        if (item == null) {
            return null;
        }

        // initialize the time with the key and basic data
        item.init(key, suffix, exptime, valueLength);

        return item;
    }

    /**
     * Find a free item slot or create a new item slot.
     */
    private Item findFreeItem(long totalLength)
    {
        SlabManager slabManager = slabAllocator.selectSlabManager(totalLength);
        if (slabManager == null) {
            return null;
        }
        ItemLru lru = lrus.get(slabManager.getId());

        // do a quick check if we have any expired items
        Item item = lru.findExpired(50, current_time);
        if (item != null) {
            // remove from hash
            assoc.delete(item.getKey());

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

        item = lru.freeLeastRecentlyUsed(50, current_time);
        if (item != null) {
            // remove from hash
            assoc.delete(item.getKey());

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
        item = lru.tryTailRepair(50, current_time);
        if (item != null) {
            // remove from hash
            assoc.delete(item.getKey());

            return item;
        }

        item = Item.createItem(totalLength, slabManager, useCas);
        return item;
    }

    /**
     * Get with expiriation logic. wrapper around assoc_find which does the lazy
     * expiration logic
     */
    public Item get(Region key)
    {
        Item item = assoc.find(key);
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
        Item item = assoc.find(key);
        if (item != null) {
            item.addReference();
        }
        return item;
    }

    /**
     * Insert
     */
    public int insert(Item item)
    {
        monitor.itemLink(item);

        // make sure the item is not liked or slabbed
        assert !item.isLinked();
        assert !item.isSlabbed();

        // 1MB max size
        assert item.getValueLength() < 1024 * 1024;

        // mark the item linked
        item.setLinked(true);
        // set the last accessed time
        item.setTime(current_time);
        // add to hash
        assoc.insert(item);

        // Allocate a new CAS ID on link.
        // set the compare and swap id
        if (useCas) {
            item.setCas(CAS_ID.incrementAndGet());
        }

        // add to lru
        lrus.get(item.getSlabId()).add(item);

        return 1;
    }

    /**
     * Delete. Remove and free item.
     */
    public void remove(Item item)
    {
        monitor.itemUnlink(item);

        // if item is still in this map
        if (item.isLinked()) {
            // remove from hash
            assoc.delete(item.getKey());

            // remove from LRU
            lrus.get(item.getSlabId()).remove(item);

            // if ref count is 0, free it; otherwise someone is still using it
            if (item.getRefCount() == 0) {
                item.free(slabAllocator.getSlabManager(item.getSlabId()));
            }
        }
    }

    /**
     * remove old and insert new
     */
    public int replace(Item oldItem, Item newItem)
    {
        monitor.itemReplace(oldItem, newItem);

        assert !oldItem.isSlabbed();

        // todo don't fire monitor events
        remove(oldItem);
        return insert(newItem);
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
        monitor.itemUpdate(item);

        lrus.get(item.getSlabId()).touch(item, current_time);
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

        for (ItemLru lru : lrus) {
            lru.flushExpired(oldest_live, assoc);
        }
    }

    //
    // Random unconverted junk
    //

//    /**
//     * Returns true if an item will fit in the cache (its size does not exceed
//     * the maximum for a cache entry.)
//     */
//    bool item_size_ok(const size_t nkey, const int flags, const int nbytes)
//    {
//        char prefix[
//        40];
//        uint8_t nsuffix;
//
//        return slabs_clsid(item_make_header(nkey + 1, flags, nbytes,
//                prefix, & nsuffix))!=0;
//    }


///*@null@*/
//    char*
//
//    do_item_cachedump(const unsigned int slabId, const unsigned int limit, unsigned int*bytes)
//    {
//        unsigned
//        int memlimit = 2 * 1024 * 1024;   /* 2MB max response size */
//        char*buffer;
//        unsigned
//        int bufcurr;
//        item * it;
//        unsigned
//        int len;
//        unsigned
//        int shown = 0;
//        char key_temp[
//        KEY_MAX_LENGTH + 1];
//        char temp[
//        512];
//
//        it = heads[slabId];
//
//        buffer = malloc((size_t) memlimit);
//        if (buffer == 0) return NULL;
//        bufcurr = 0;
//
//        while (it != NULL && (limit == 0 || shown < limit)) {
//            assert (it.nkey <= KEY_MAX_LENGTH);
//            /* Copy the key since it may not be null-terminated in the struct */
//            strncpy(key_temp, ITEM_key(it), it.nkey);
//            key_temp[it.nkey] = 0x00; /* terminate */
//            len = snprintf(temp, sizeof(temp), "ITEM %s [%d b; %lu s]\r\n",
//                    key_temp, it.nbytes - 2,
//                    (unsignedlong)it.exptime + process_started);
//            if (bufcurr + len + 6 > memlimit)  /* 6 is END\r\n\0 */ {
//                break;
//            }
//            memcpy(buffer + bufcurr, temp, len);
//            bufcurr += len;
//            shown++;
//            it = it.next;
//        }
//
//        memcpy(buffer + bufcurr, "END\r\n", 6);
//        bufcurr += 5;
//
//        *bytes = bufcurr;
//        return buffer;
//    }
//
//    void do_item_stats(ADD_STAT add_stats, void*c)
//    {
//        int i;
//        for (i = 0; i < LARGEST_ID; i++) {
//            if (tails[i] != NULL) {
//                const char*fmt = "items:%d:%s";
//                char key_str[
//                STAT_KEY_LEN];
//                char val_str[
//                STAT_VAL_LEN];
//                int klen = 0, vlen = 0;
//
//                APPEND_NUM_FMT_STAT(fmt, i, "number", "%u", sizes[i]);
//                APPEND_NUM_FMT_STAT(fmt, i, "age", "%u", tails[i].time);
//                APPEND_NUM_FMT_STAT(fmt, i, "evicted",
//                        "%u", itemstats[i].evicted);
//                APPEND_NUM_FMT_STAT(fmt, i, "evicted_nonzero",
//                        "%u", itemstats[i].evicted_nonzero);
//                APPEND_NUM_FMT_STAT(fmt, i, "evicted_time",
//                        "%u", itemstats[i].evicted_time);
//                APPEND_NUM_FMT_STAT(fmt, i, "outofmemory",
//                        "%u", itemstats[i].outofmemory);
//                APPEND_NUM_FMT_STAT(fmt, i, "tailrepairs",
//                        "%u", itemstats[i].tailrepairs);
//                ;
//            }
//        }
//
//        /* getting here means both ascii and binary terminators fit */
//        add_stats(NULL, 0, NULL, 0, c);
//    }
//
//    /**
//     * dumps out a list of objects of each size, with granularity of 32 bytes
//     */
//    void do_item_stats_sizes(ADD_STAT add_stats, void*c)
//    {
//
//        /* max 1MB object, divided into 32 bytes size buckets */
//        const int num_buckets = 32768;
//        unsigned int*histogram = calloc(num_buckets, sizeof(int));
//
//        if (histogram != NULL) {
//            int i;
//
//            /* build the histogram */
//            for (i = 0; i < LARGEST_ID; i++) {
//                item * iter = heads[i];
//                while (iter) {
//                    int ntotal = ITEM_ntotal(iter);
//                    int bucket = ntotal / 32;
//                    if ((ntotal % 32) != 0) bucket++;
//                    if (bucket < num_buckets) histogram[bucket]++;
//                    iter = iter.next;
//                }
//            }
//
//            /* write the buffer */
//            for (i = 0; i < num_buckets; i++) {
//                if (histogram[i] != 0) {
//                    char key[
//                    8];
//                    int klen = 0;
//                    klen = snprintf(key, sizeof(key), "%d", i * 32);
//                    assert (klen < sizeof(key));
//                    APPEND_STAT(key, "%u", histogram[i]);
//                }
//            }
//            free(histogram);
//        }
//        add_stats(NULL, 0, NULL, 0, c);
//    }

}
