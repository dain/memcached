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
package org.iq80.memcached;

import org.iq80.memcached.Item.PrevChain;

public class ItemLru {
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

    private final SlabManager slabManager;
    private final boolean evictToFree;
    private final Monitor monitor;
    private final ItemStats stats;
    private long head;
    private long tail;
    private long size;

    public ItemLru(SlabManager slabManager, boolean evictToFree, Monitor monitor, ItemStats stats) {
        this.slabManager = slabManager;
        this.evictToFree = evictToFree;
        this.monitor = monitor;
        this.stats = stats;
    }

    public long size() {
        return size;
    }

    /**
     * Check if there are any expired items on the tail.
     */
    public Item findExpired(int tries, int currentTime) {
        // do a quick check if we have any expired items in the tail..
        for (Item search : new PrevChain(slabManager.getAllocator(), tail)) {
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
    public Item freeLeastRecentlyUsed(int tries, int currentTime) {
        if (!evictToFree) {
            stats.outOfMemory();
            return null;
        }

        if (tail == 0) {
            stats.outOfMemory();
            return null;
        }

        for (Item search : new PrevChain(slabManager.getAllocator(), tail)) {
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
     * Search for an item that has been locked for more then 3 hours, which is a
     * bug, and frees that item.
     */
    public Item tryTailRepair(int tries, int currentTime) {
        // Last ditch effort. There is a very rare bug which causes
        // ref count leaks. We've fixed most of them, but it still happens,
        // and it may happen in the future.
        // We can reasonably assume no item can stay locked for more than
        // three hours, so if we find one in the tail which is that old,
        // free it anyway.
        for (Item search : new PrevChain(slabManager.getAllocator(), tail)) {
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

    /**
     * Adds item to this LRU.
     */
    public void add(Item item) {
        // item is the new head
        assert item.getSlabId() == slabManager.getId();
        assert item.getAddress() != head;

        stats.added(item);

        item.insertAfter(head);

        // set head
        head = item.getAddress();

        // if list was empty, set the tail also
        if (tail == 0) {
            tail = item.getAddress();
        }

        // update size
        size++;
    }

    /**
     * Remove item from this LRU.
     */
    public void remove(Item item) {
        // verify we have a valid item
        assert item.getSlabId() == slabManager.getId();

        stats.removed(item);

        // if this is the head item, set head to item.next
        if (head == item.getAddress()) {
            assert (item.getPrev() == 0);
            head = item.getNext();
        }

        // if this is the tail item, set tail to item.prev
        if (tail == item.getAddress()) {
            assert (item.getNext() == 0);
            tail = item.getPrev();
        }


        item.unlink();

        size--;
    }

    /**
     * Sets the last modified time of the item.
     */
    public void touch(Item item, int currentTime) {
        // if item hasn't been moved in 60 seconds, move to head of lru
        if (item.getTime() < currentTime - ITEM_UPDATE_INTERVAL) {
            assert !item.isSlabbed();

            if (item.isLinked()) {
                remove(item);
                item.setTime(currentTime);
                add(item);
            }
        }
    }

    private void steal(Item item) {
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
     * Walks entire cache, freeing expired items. expires items that are more
     * recent than the oldest_live setting.
     */
    public void flushExpired(long oldestLive, Association assoc) {
        // The LRU is sorted in decreasing time order, and an item's timestamp
        // is never newer than its last access time, so we only need to walk
        // back until we hit an item older than the oldest_live time.
        // The oldest_live checking will auto-expire the remaining items.
        long nextAddress;
        for (Item item = Item.cast(slabManager.getAllocator(), head); item != null; item.setAddress(slabManager.getAllocator(), nextAddress)) {
            if (item.getTime() < oldestLive) {
                // We've hit the first old item. Continue to the next queue.
                break;
            }

            // this is weird but we need to save off the next address and
            // manually move the pointer to avoid a lot of unnecessary o
            // object creation
            nextAddress = item.getNext();

            // if the item is not already slabbed, free it
            if (!item.isSlabbed()) {
                monitor.itemUnlink(item);

                // if item is still in this map
                if (item.isLinked()) {
                    stats.removed(item);

                    // remove from hash
                    assoc.delete(item.getKey());

                    // remove from LRU
                    remove(item);

                    // if ref count is 0, free it; otherwise someone is still using it
                    if (item.getRefCount() == 0) {
                        // head to tail item cannot be freed, unlink first
                        assert item.getAddress() != head;
                        assert item.getAddress() != tail;

                        item.free(slabManager);
                    }
                }
            }
        }
    }
}
