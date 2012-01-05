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

import org.iq80.memcached.Item.HashChain;
import org.iq80.memory.Allocator;
import org.iq80.memory.Region;

public class Association
{
    private static int hashSize(int n)
    {
        return 1 << n;
    }

    private static int hashMask(int n)
    {
        return hashSize(n) - 1;
    }

    private final Monitor monitor;

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
    private long[] oldHashtable;

    /**
     * Number of items in the hash table.
     */
    private int hashItems = 0;

    /**
     * Are we in the middle of expanding now?
     */
    private boolean expanding;

    /**
     * During expansion we migrate values with bucket granularity; this is how
     * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
     */
    private int expandBucket = 0;


    public Association(Monitor monitor)
    {
        this(monitor, 16);
    }

    public Association(Monitor monitor, int hashPower)
    {
        this.monitor = monitor;
        this.hashPower = hashPower;
        primaryHashtable = new long[hashSize(hashPower)];
    }

    public Item find(Region key)
    {
        int hashCode = Hash.hash(key, 0);

        if (expanding) {
            int oldBucket = hashCode & hashMask(hashPower - 1);
            if (oldBucket >= expandBucket) {
                return findFromBucket(key, oldHashtable, oldBucket);
            }
        }

        return findFromBucket(key, primaryHashtable, hashCode & hashMask(hashPower));
    }

    private Item findFromBucket(Region key, long[] hashtable, int bucket)
    {
        int depth = 0;
        for (Item item : new HashChain(key.getAllocator(), hashtable[bucket])) {
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
    public void insert(Item item)
    {

        // shouldn't have duplicately named things defined
        assert find(item.getKey()) == null;

        int hashCode = Hash.hash(item.getKey(), 0);

        if (expanding) {
            int oldbucket = hashCode & hashMask(hashPower - 1);
            if (oldbucket >= expandBucket) {
                insertInBucket(item, oldHashtable, oldbucket);
                return;
            }
        }

        insertInBucket(item, primaryHashtable, hashCode & hashMask(hashPower));
    }

    private void insertInBucket(Item item, long[] hashtable, int bucket)
    {
        item.setHashClainNext(hashtable[bucket]);
        hashtable[bucket] = item.getAddress();

        hashItems++;
        if (!expanding && hashItems > (hashSize(hashPower) * 3) / 2) {
            expand();
        }
        monitor.assocInsert(item, hashItems);
    }

    public boolean delete(Region key)
    {
        //
        // This would be way easier with dummy nodes
        //

        int hashCode = Hash.hash(key, 0);

        if (expanding) {
            int oldbucket = hashCode & hashMask(hashPower - 1);
            if (oldbucket >= expandBucket) {
                return deleteFromBucket(key, oldHashtable, oldbucket);
            }
            // fallthrough
        }

        return deleteFromBucket(key, primaryHashtable, hashCode & hashMask(hashPower));
    }

    private boolean deleteFromBucket(Region key, long[] hashtable, int bucket)
    {
        // If we didn't find anything, just stop here
        if (hashtable[bucket] != 0) {
            return false;
        }

        Item item = Item.cast(key.getAllocator(), hashtable[bucket]);

        // If this is the one, delete it from the table
        if (item.keyEquals(key)) {
            hashItems--;
            if (item.getHashClainNext() != 0) {
                hashtable[bucket] = item.getHashClainNext();
            }
            item.setHashClainNext(0);
            return true;
        }

        // search though the hash chain...
        Item before = item;
        for (Item next : new HashChain(before.getAllocator(), before.getHashClainNext())) {
            // If this is the one, delete it from the chain
            if (next.keyEquals(key)) {
                hashItems--;
                before.setHashClainNext(next.getHashClainNext());
                next.setHashClainNext(0); // pointless by why not
                return true;
            }
            // remember the previous item in the chain so we can update the hashChainNext pointer
            before.setAddress(next.getAllocator(), next.getAddress());
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
        oldHashtable = primaryHashtable;

        // create the new primary hashtable
        primaryHashtable = new long[hashSize(hashPower + 1)];

        // log expansion
//        if (settings.verbose > 1) {
//            fprintf(stderr, "Hash table expansion starting\n");
//        }
        hashPower++;
        expanding = true;
        expandBucket = 0;

        // wake expander thread
//            pthread_cond_signal( & maintenance_cond);
    }


    static volatile boolean do_run_maintenance_thread = true;

    public static final int DEFAULT_HASH_BULK_MOVE = 1;

    int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

    private void associateMaintenanceThread(Allocator allocator)
    {

        while (do_run_maintenance_thread) {
            // Lock the cache, and bulk move multiple buckets to the new hash table.
//            pthread_mutex_lock( & cache_lock);

            // migrate a batch of buckets
            for (int i = 0; i < hash_bulk_move && expanding; ++i) {
                for (Item next, item = Item.cast(allocator, oldHashtable[expandBucket]); null != item; item = next) {
                    // remember the next because it will be overwritten below
                    next = Item.cast(item.getAllocator(), item.getHashClainNext());

                    // rehash
                    int bucket = Hash.hash(item.getKey(), 0) & hashMask(hashPower);

                    // move item
                    item.setHashClainNext(primaryHashtable[bucket]);
                    primaryHashtable[bucket] = item.getAddress();
                }

                // clear the address in the old table
                oldHashtable[expandBucket] = 0;

                expandBucket++;
                if (expandBucket == hashSize(hashPower - 1)) {
                    expanding = false;
//                    free(old_hashtable);
                    oldHashtable = null;
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

}
