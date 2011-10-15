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
