#include "Tree.h"
#include "RdmaBuffer.h"
#include "Timer.h"
#include "Node.h"

#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
#include <atomic>
#include <mutex>

// uint64_t entry_num_in_inner_node[MAX_APP_THREAD][256];
// uint64_t entry_num_in_leaf_node[MAX_APP_THREAD][kLeafCardinality];

double cache_miss[MAX_APP_THREAD];
double cache_hit[MAX_APP_THREAD];
uint64_t lock_fail[MAX_APP_THREAD];
// uint64_t try_lock[MAX_APP_THREAD];
uint64_t write_handover_num[MAX_APP_THREAD];
uint64_t try_write_op[MAX_APP_THREAD];
uint64_t read_handover_num[MAX_APP_THREAD];
uint64_t try_read_op[MAX_APP_THREAD];
uint64_t read_leaf_retry[MAX_APP_THREAD];
uint64_t leaf_cache_invalid[MAX_APP_THREAD];
uint64_t try_read_leaf[MAX_APP_THREAD];
uint64_t read_node_repair[MAX_APP_THREAD];
uint64_t try_read_node[MAX_APP_THREAD];
uint64_t read_node_type[MAX_APP_THREAD][MAX_NODE_TYPE_NUM];
uint64_t latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
volatile bool need_stop = false;
uint64_t retry_cnt[MAX_APP_THREAD][MAX_FLAG_NUM];

thread_local CoroCall Tree::worker[MAX_CORO_NUM];
thread_local CoroCall Tree::master;
thread_local CoroQueue Tree::busy_waiting_queue;


Tree::Tree(DSM *dsm, uint16_t tree_id) : dsm(dsm), tree_id(tree_id) {
  assert(dsm->is_register());

  index_cache = new RadixCache(define::kIndexCacheSize, dsm);

  local_lock_table = new LocalLockTable();

  root_ptr_ptr = get_root_ptr_ptr();

  // init root entry to Null
  auto entry_buffer = (dsm->get_rbuf(0)).get_entry_buffer();
  dsm->read_sync((char *)entry_buffer, root_ptr_ptr, sizeof(InternalEntry));
  auto root_ptr = *(InternalEntry *)entry_buffer;
  if (dsm->getMyNodeID() == 0 && root_ptr != InternalEntry::Null()) {
    auto cas_buffer = (dsm->get_rbuf(0)).get_cas_buffer();
retry:
    bool res = dsm->cas_sync(root_ptr_ptr, (uint64_t)root_ptr, (uint64_t)InternalEntry::Null(), cas_buffer);
    if (!res && (root_ptr = *(InternalEntry *)cas_buffer) != InternalEntry::Null()) {
      goto retry;
    }
  }
}


GlobalAddress Tree::get_root_ptr_ptr() {
  GlobalAddress addr;
  addr.nodeID = 0;
  addr.offset = define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;
  return addr;
}


InternalEntry Tree::get_root_ptr(CoroContext *cxt, int coro_id) {
  auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
  dsm->read_sync((char *)entry_buffer, root_ptr_ptr, sizeof(InternalEntry), cxt);
  return *(InternalEntry *)entry_buffer;
}

// Insert kv
void Tree::insert(const Key &k, Value v, CoroContext *cxt, int coro_id, bool is_update, bool is_load) {
  bool find_error = false;
  int log_level = 5;
  // if (dsm->getMyThreadID() == 0) find_error = true; // true for debug
  if(find_error) printf("\n Thread: %u insert: %lu\n", dsm->getMyThreadID(), key2int(k));
  assert(dsm->is_register());

  // handover
  bool write_handover = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);

  // traversal
  GlobalAddress p_ptr;
  InternalEntry p;
  GlobalAddress node_ptr;  // node address(excluding header)
  uint8_t depth;
  int retry_flag = FIRST_TRY;

  // cache
  bool from_cache = false;
  volatile CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  int entry_idx = -1; // index of target entry in current inner node
  int cache_depth = 0;

  // temp
  char* page_buffer;
  bool is_valid, type_correct;
  InternalPage* p_node = nullptr;
  Header hdr;
  int max_num;
  uint64_t* cas_buffer;
  uint64_t* faa_buffer;
  int debug_cnt = 0;

  lock_res = local_lock_table->acquire_local_write_lock(k, v, &busy_waiting_queue, cxt, coro_id);
  write_handover = (lock_res.first && !lock_res.second);
  try_write_op[dsm->getMyThreadID()]++;
  if (write_handover) {
    write_handover_num[dsm->getMyThreadID()]++;
    goto insert_finish;
  }

  // search local cache
  from_cache = index_cache->search_from_cache(k, entry_ptr_ptr, entry_ptr, entry_idx); // noting: depth maybe outdated
  // from_cache = false; // false for close cache
  if (from_cache) { // cache hit
    if(find_error) printf("cache hit!\n");
    assert(entry_idx >= 0);
    p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
    p = entry_ptr->records[entry_idx];
    node_ptr = entry_ptr->addr;
    depth = entry_ptr->depth;
  }
  else {
    entry_idx = -1;
    p_ptr = root_ptr_ptr;
    p = get_root_ptr(cxt, coro_id);
    depth = 0;
  }

  depth ++;  // partial key in entry is matched
  cache_depth = depth;

  UNUSED(is_update);  // is_update is only used in ROWEX_ART baseline

next:
  retry_cnt[dsm->getMyThreadID()][retry_flag] ++;
  if (find_error) printf("insert: %lu, depth: %d\n", key2int(k), depth);
  // 1. The system has just started, create root node
  if ((dsm->getMyThreadID() == 0) && (p == InternalEntry::Null())) {
    if(find_error) printf("creating root ...\n");
    assert(from_cache == false);
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    bool res = write_root_node(k, v, get_partial(k, depth-1), p_ptr, p, cas_buffer, cxt, coro_id);

    assert(res);
    if(find_error) printf("create root !!!\n");
    goto insert_finish;
  } else if (p == InternalEntry::Null()) {
    entry_idx = -1;
    node_ptr = root_ptr_ptr;
    p_ptr = root_ptr_ptr;
    p = get_root_ptr(cxt, coro_id);
    depth = 1;
    if(find_error) printf("waiting for root node\n");
    goto next;
  }

  // 2. If we are at a leaf, we need to write (update/insert/split) target kv
  if (p.is_leaf) {
    // 2.1 read the leaf
    if(find_error) printf("find a leaf\n");    
    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
    auto faa_buffer = (dsm->get_rbuf(coro_id)).get_faa_buffer();
    is_valid = faa_and_read(p.addr(), leaf_buffer, p_ptr, from_cache, faa_buffer, cxt, coro_id);

    int64_t lock = (int64_t)(*faa_buffer);
    if (lock < define::SMOUpLimit) { // current leaf node is undergoing SMO, waiting for the completion of SMO
      dsm->faa_sync(GADD(p.addr(), lock_offset), -define::normalOp, faa_buffer, cxt); // restore the state of the node lock

      lock = (int64_t)(*faa_buffer);
      while (lock < define::SMOUpLimit) { // read the latest state of node lock
        dsm->read_sync((char*)faa_buffer, GADD(p.addr(), lock_offset), sizeof(uint64_t), cxt);
        lock = (int64_t)(*faa_buffer);
        if(log_level >= 10) printf("Thread %u: Key: %lu, IDU wait for SMO, lock: %ld\n", dsm->getMyThreadID(), key2int(k), lock);
      }
      retry_flag = INVALID_LEAF;
      leaf_cache_invalid[dsm->getMyThreadID()] ++;      
      goto next; // SMO completed, retry insert
    }

    auto leaf = (Leaf *)leaf_buffer;

    if (!is_valid) { // outdated cache entry in cached node: new inner nodes have been inserted as parent nodes of current leaf node
      auto faa_buffer = (dsm->get_rbuf(coro_id)).get_faa_buffer();
      dsm->faa_sync(GADD(p.addr(), lock_offset), -define::normalOp, faa_buffer, cxt); // restore the state of the node lock

      // invalidate the old leaf entry cache
      if (from_cache) {
        if(find_error) printf("invalidate! 200\n");
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }
      // re-read leaf entry
      auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
      dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
      p = *(InternalEntry *)entry_buffer;
      from_cache = false;
      retry_flag = INVALID_LEAF;
      leaf_cache_invalid[dsm->getMyThreadID()] ++;
      if(find_error) printf("leaf invalid!! valid_byte: %d, from_cache: %d, p_ptr: %lu, rev_ptr: %lu\n", leaf->valid_byte, from_cache, p_ptr.val, leaf->rev_ptr.val);
      goto next;
    }

    assert(leaf->rev_ptr == p_ptr);

    auto min = leaf->min;
    auto max = leaf->max;
    bool upper = (max.back() == 255) ? true : false;
    // 2.2 After reaching a leaf, there are three possibilities: 
    // (1) If it falls within the range, perform an insertion. 
    // (2) If it exceeds the range represented by max. 
    // (3) If it is less than the range represented by min.
    if (depth == define::keyLen+1) { // the prefix has been fully expanded
      if(find_error) printf("leaf node depth = 9\n");

      if (k < max || upper) { // case 1, write data items to the leaf node
        if(find_error) printf("write in leaf 227\n");
        goto write_in_leaf;
      } else if (k >= max) { // case 2, reread the upper-level special inner node, then search for the latest leaf node, and proceed with further writing into it
        if(find_error) printf("reread father, k: %lu, entry_idx: %d, min: %lu, max: %lu, rev_ptr: %lu, p_ptr: %lu\n", key2int(k), entry_idx, key2int(min), key2int(max), leaf->rev_ptr.val, p_ptr.val);
        auto faa_buffer = (dsm->get_rbuf(coro_id)).get_faa_buffer();
        dsm->faa_sync(GADD(p.addr(), lock_offset), -define::normalOp, faa_buffer, cxt); // not target leaf node, restore the state of the node lock

        if (from_cache) {
          index_cache->invalidate(entry_ptr_ptr, entry_ptr);
          if(find_error) printf("invalidate! 236\n");
        }
        page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
        dsm->read_sync(page_buffer, GSUB(node_ptr, sizeof(GlobalAddress) + sizeof(Header)), define::allocationPageSize - sizeof(InternalEntry)*240, cxt); // 后续衡量一下读取的size
        p_node = (InternalPage *)page_buffer;
        if (depth-1 == p_node->hdr.depth + p_node->hdr.partial_len) {
          index_cache->add_to_cache(k, p_node, node_ptr);
          if(find_error) printf("add to cache!, key: %lu, node_type: %d, slot0: %u, slot1: %u, slot2: %u, slot3: %u, slot4: %u, slot5: %u\n", key2int(k), p_node->hdr.node_type, p_node->records[0].partial, p_node->records[1].partial, p_node->records[2].partial, p_node->records[3].partial, p_node->records[4].partial, p_node->records[5].partial);
        }

        uint8_t target = 0;
        uint8_t pos = 0;
        bool find = false;
        for (int i = 0; i < 16; ++ i) { // the number of entries in the special inner node is limited to 16
          if (p_node->records[i].val == 0) {
            break;
          }
          const auto& e = p_node->records[i];
          if (target == 0 && e.partial == target) { // prefix = 0
            pos = i;
            find = true;
          } else if (e.partial <= k.at(define::keyLen-1) && e.partial > target) {
            target = e.partial;
            pos = i;
            find = true;
          }
        }
        assert(find);
        if(find_error) printf("find new and exact leaf node, entry_idx: %d, target_pos: %d, partial0: %d,addr: %lu | partial1: %d,addr: %lu | partial2: %d,addr: %lu | partial3: %d,addr: %lu\n", entry_idx, pos, p_node->records[0].partial,p_node->records[0].next_addr, p_node->records[1].partial,p_node->records[1].next_addr, p_node->records[2].partial,p_node->records[2].next_addr, p_node->records[3].partial,p_node->records[3].next_addr);

        if (pos > node_type_to_num(p_node->hdr.type())) { // to be deleted
          auto next_type = num_to_node_type(pos);
          cas_node_type(next_type, p_ptr, p, hdr, cxt, coro_id);
        }

        entry_idx = pos;
        p_ptr = GADD(node_ptr, sizeof(InternalEntry) * entry_idx);
        p = p_node->records[entry_idx];
        from_cache = false;
        retry_flag = INVALID_LEAF;
        leaf_cache_invalid[dsm->getMyThreadID()] ++;
        if(find_error) printf("error leaf! 277\n");
        goto next;
      } else if (k < min || ((uint32_t)longest_common_prefix(min, k, 0) < define::keyLen)) { // case 3, incomplete logic may lead to this situation, we keep this codes to discover and handle extreme cases
        assert(false);
        if (from_cache) {
          if(find_error) printf("invalidate! 282\n");
          index_cache->invalidate(entry_ptr_ptr, entry_ptr);
        }
        auto faa_buffer = (dsm->get_rbuf(coro_id)).get_faa_buffer();
        dsm->faa_sync(GADD(p.addr(), lock_offset), -define::normalOp, faa_buffer, cxt); // restore the state of the node lock

        entry_idx = -1;
        node_ptr = root_ptr_ptr; // traverse from the root node
        p_ptr = root_ptr_ptr;
        p = get_root_ptr(cxt, coro_id);
        depth = 1; // depth is 1 but not 0
        from_cache = false;
        retry_flag = INVALID_LEAF;
        leaf_cache_invalid[dsm->getMyThreadID()] ++;
        if(find_error) printf("error leaf! 296, min: %lu, k: %lu\n", key2int(min), key2int(k));
        goto next;
      }
      assert(false);
    } else if (depth < define::keyLen+1) { // the prefix is not fully expanded

      if(find_error) printf("leaf node depth < 9, depth: %d\n", depth);
      if ((k >= min && k < max) || (upper && (k >= min && k <= max))) { // in the range of current leaf node
        if(find_error) printf("write in leaf 304\n");
        goto write_in_leaf;
      } else { // Upgrade an inner node, then connect the old node and the new leaf node to it. 
               // Note: If a concurrent conflict occurs, consider reclaiming the allocated space for the newly created leaf node😁
        int partial_len = longest_common_prefix(min, k, depth);
        uint8_t diff_partial = get_partial(min, depth + partial_len);
        auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
        if(find_error) printf("out_of_place_write_node 311\n");
        bool res = out_of_place_write_node(k, v, depth, partial_len, diff_partial, p_ptr, p, node_ptr, cas_buffer, cxt, coro_id);

        // cas fail, retry
        if (!res) {
          auto faa_buffer = (dsm->get_rbuf(coro_id)).get_faa_buffer();
          dsm->faa_sync(GADD(p.addr(), lock_offset), -define::normalOp, faa_buffer, cxt); // restore the state of the node lock

          if (from_cache) {
            index_cache->invalidate(entry_ptr_ptr, entry_ptr);
          }
          p = *(InternalEntry*) cas_buffer; // p is outdated, use the returned value of CAS to update p 
          from_cache = false;
          retry_flag = CAS_LEAF;
          if(find_error) printf("concurrent conflict 325\n");
          
          goto next;
        }

        if (from_cache) {
          if(find_error) printf("invalidate! 331\n");
          index_cache->invalidate(entry_ptr_ptr, entry_ptr);
        }

        auto faa_buffer = (dsm->get_rbuf(coro_id)).get_faa_buffer();
        dsm->faa_sync(GADD(p.addr(), lock_offset), -define::normalOp, faa_buffer, cxt); // operation successful, restore the state of the node lock

        if(find_error) printf("stop here 338\n");
        goto insert_finish;
      }
    } else {
      assert(false);
    }

write_in_leaf:
      // 2.3 update/insert/split
      uint64_t entry_cnt = 0;
      int empty_index = -1;
      char *value_addr = nullptr;
      char *key_addr = nullptr;  
      bool is_update = false;   // to distinguish the operation for space recycle

      for (int i = 0; i < kLeafCardinality; ++i) {
        Key rkey = leaf->keys[i];
        if (key2int(rkey) != 0) {
          entry_cnt ++;
          if (rkey == k) {
            leaf->values[i] = v;
            value_addr = (char *)&(leaf->values[i]);
            is_update = true; // update
            break;
          }
        } else if (empty_index == -1) {
          empty_index = i;
        }
      }
      assert(entry_cnt != kLeafCardinality);

      if (value_addr == nullptr) { // insert
        if (empty_index == -1) {
          assert(false);
        }
        leaf->keys[empty_index] = k;
        leaf->values[empty_index] = v;
        key_addr = (char *)&(leaf->keys[empty_index]);
        value_addr = (char *)&(leaf->values[empty_index]);

        entry_cnt ++;
      }
retry:
      bool need_split = entry_cnt == kLeafCardinality; // current leaf node is full 

      if (!need_split) { // update or insert, write target kv into the update_addr
        if(find_error) printf("write kv, entry_cnt: %lu, empty_index: %d, is_update: %d, kLeafCardinality: %d, k: %lu, min: %lu, max: %lu\n", entry_cnt, empty_index, (int)is_update, kLeafCardinality, key2int(k), key2int(leaf->min), key2int(leaf->max));
        
        if (!is_update) { // cas key
          if(find_error) printf("insert: cas key\n");
          auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
          GlobalAddress target_key_addr = GADD(p.addr(), (key_addr - leaf_buffer));
          dsm->cas_sync(target_key_addr, 0UL, key2int_inverse(k), cas_buffer, cxt);
          if ((*cas_buffer != 0UL) && (*cas_buffer != key2int(k))) { // cas failed, which mean that the empty slot has been used by others. 
            leaf->keys[empty_index] = int2key(*cas_buffer);
            key_addr += sizeof(Key);
            value_addr += sizeof(Value);
            empty_index ++;
            entry_cnt ++;
            goto retry;
          }
        }
        GlobalAddress target_value_addr = GADD(p.addr(), (value_addr - leaf_buffer)); // the offset is fixed
        dsm->write(value_addr, target_value_addr, sizeof(Value), false, cxt); // write target value into target_value_addr
        auto faa_buffer = (dsm->get_rbuf(coro_id)).get_faa_buffer();
        dsm->faa(GADD(p.addr(), lock_offset), -define::normalOp, faa_buffer, false, cxt); // operation successful, restore the state of the node lock

        if(find_error) printf("stop here 405\n");
        goto insert_finish;
      } else { // split (one case of SMO)
        if(find_error) printf("split\n");
        auto faa_buffer = (dsm->get_rbuf(coro_id)).get_faa_buffer();
        dsm->faa_sync(GADD(p.addr(), lock_offset), define::SMOstatus, faa_buffer, cxt); // try to lock the page for SMO

        lock = (int64_t)(*faa_buffer);
        if (lock < define::SMOUpLimit) { // someone is SMO , we should wait
          dsm->faa_sync(GADD(p.addr(), lock_offset), -define::SMOstatus-define::normalOp, faa_buffer, cxt);
          lock = (int64_t)(*faa_buffer);
          while(lock < define::SMOUpLimit){ 
            dsm->read_sync((char*)faa_buffer, GADD(p.addr(), lock_offset), sizeof(uint64_t), cxt); // reread lock
            lock = (int64_t)(*faa_buffer);
            if(log_level >= 10) printf("Thread %d: Key: %ld, SMO wait for SMO, lock: %ld\n", dsm->getMyThreadID(), key2int(k), lock);
          }
          // after the last SMO, there are two scenarios (one of which results in a new internal node), reload the node pointed to by p_ptr
          auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
          dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
          p = *(InternalEntry *)entry_buffer;
          from_cache = false;
          retry_flag = INVALID_LEAF;
          leaf_cache_invalid[dsm->getMyThreadID()] ++;
          goto next;
        }

        // we get the priority to SMO
        while(lock != (define::SMOstatus + define::normalOp)){
          dsm->read_sync((char*)faa_buffer, GADD(p.addr(), lock_offset), sizeof(uint64_t), cxt); // reread lock
          lock = (int64_t)(*faa_buffer);     
          if(log_level >= 10) printf("Thread %d: Key: %lu, SMO wait for IDU, lock: %ld\n", dsm->getMyThreadID(), key2int(k), lock);    
        }
        dsm->read_sync(leaf_buffer, p.addr(), define::kLeafPageSize, cxt); // reread the latest leaf node
        leaf = (Leaf *)leaf_buffer;
        leaf->keys[kLeafCardinality-1] = k;
        leaf->values[kLeafCardinality-1] = v;

        std::sort(
            leaf->keys, leaf->keys + kLeafCardinality,
            [](const Key &a, const Key &b) { return a < b; }); // Sort the keys in ascending order. Todo: Sort the corresponding values as well

        Key split_key;
        auto new_leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();

        int m = entry_cnt / 4 * 3; // reduce space wastage
        split_key = leaf->keys[m];
        assert(split_key > leaf->min);
        assert(split_key < leaf->max);

        auto new_leaf = new (new_leaf_buffer) Leaf(GlobalAddress::Null()); // new leaf node
        new_leaf->min = split_key;
        new_leaf->max = leaf->max;
        leaf->max = split_key;

        // move data
        for (int i = m; i < entry_cnt; ++i) {
          new_leaf->keys[i - m] = leaf->keys[i];
          new_leaf->values[i - m] = leaf->values[i];
          leaf->keys[i] = int2key(0);
          leaf->values[i] = 0;
        }
        // Find the position for inserting the new leaf node and link the leaf node to the index. This is because sibling_ptr is not used, 
        // and there is no guarantee that the new leaf node can be found after writing the original leaf node and unlocking, 
        // it is necessary to place this step within the scope of the lock on the original leaf node

        // The insertion of a new leaf node involves two cases:
        // (1) If the depth of the original leaf is equal to define::keyLen+1, indicating that the internal nodes have already represented all prefixes, 
        // then the new leaf is inserted into the upper-level internal node of the original leaf.
        // (2) If the depth of the original leaf is less than define::keyLen+1, meaning that a portion of the prefixes is hidden in the original leaf, 
        // it is necessary to upgrade to a special internal node and connect the two leaves into it.
        if (depth == define::keyLen+1) {
          // The following operations are performed on the layer above the current depth, so when using the depth, it needs to be reduced by one
          page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
          dsm->read_sync(page_buffer, GSUB(node_ptr, sizeof(GlobalAddress) + sizeof(Header)), define::allocationPageSize - sizeof(InternalEntry)*240, cxt); // be careful with the size
          p_node = (InternalPage *)page_buffer;
          max_num = node_type_to_num(p_node->hdr.type());
          if(max_num>16) assert(false);
          for (int i = 0; i < 16; ++ i) { // the number of entries in the special inner node is limited to 16
            auto old_e = p_node->records[i];
            if (old_e == InternalEntry::Null()) {
              auto e_ptr = GADD(node_ptr, i * sizeof(InternalEntry));
              auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
              auto new_leaf_addr = dsm->alloc(define::kLeafPageSize, false);
              bool res = out_of_place_write_leaf(new_leaf_buffer, new_leaf_addr, get_partial(split_key, depth-1), e_ptr, old_e, cas_buffer, cxt, coro_id);
              // cas success, return
              if (res) {
                // update the old leaf node
                dsm->write_sync(leaf_buffer, p.addr(), lock_offset, cxt);
                auto faa_buffer = (dsm->get_rbuf(coro_id)).get_faa_buffer();
                dsm->faa(GADD(p.addr(), lock_offset), -define::normalOp-define::SMOstatus, faa_buffer, false, cxt); // operation successful, restore the state of the node lock

                if(find_error) printf("success, entry_cnt: %lu, i: %d, max_num: %d, split_key: %lu\n", entry_cnt, i, max_num, key2int(split_key));
                if (from_cache) {
                  index_cache->invalidate(entry_ptr_ptr, entry_ptr);
                }
                goto insert_finish;
              }
              // cas fail, check
              else {
                auto e = *(InternalEntry*) cas_buffer;
                if (e.partial == get_partial(split_key, depth-1)) {  // same partial keys insert to the same empty slot
                  dsm->free(new_leaf_addr, define::kLeafPageSize);
                  assert(false);
                }
              } // this empty slot is already occupied, look for the next available slot
            }
          }
          assert(false); // todo: search for an empty slot, supporting the expansion of inner node types

        } else { // Upgrade to an inner node, then connect the two leaf nodes to it. 
                 // Note: In case of concurrent conflicts, consider reclaiming the allocated space for the newly created leaf node😁
          if (from_cache) {
            index_cache->invalidate(entry_ptr_ptr, entry_ptr);
          }
          int partial_len = longest_common_prefix(leaf->min, split_key, depth);
          uint8_t diff_partial = get_partial(leaf->min, depth + partial_len);
          auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
          auto faa_buffer = (dsm->get_rbuf(coro_id)).get_faa_buffer();
          bool res = out_of_place_write_node_with_two_leaf(split_key, new_leaf_buffer, leaf_buffer, p.addr(), depth, partial_len, diff_partial, p_ptr, p, node_ptr, cas_buffer, cxt, coro_id);
          // cas fail, retry
          if (!res) {
            dsm->faa_sync(GADD(p.addr(), lock_offset), -define::normalOp-define::SMOstatus, faa_buffer, cxt); // operation failed, restore the state of the node lock

            p = *(InternalEntry*) cas_buffer; // p has expired, use teh returned value of CAS to update p
            from_cache = false;
            retry_flag = CAS_LEAF;
            leaf_cache_invalid[dsm->getMyThreadID()] ++;
            goto next;
          }
          dsm->faa(GADD(p.addr(), lock_offset), -define::normalOp-define::SMOstatus, faa_buffer, false, cxt); // operation successful, restore the state of the node lock

          if(find_error) printf("stop here 536\n");
          goto insert_finish;
        }
        goto insert_finish;
      } // split done

  } // The case where the node pointed to by p is a leaf has been processed

  // 3. Find out an inner node
  // 3.1 read the inner node
  if(find_error) printf("find an inner node, node_type: %d\n", p.type());
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node(p, type_correct, page_buffer, p_ptr, depth, from_cache, cxt, coro_id);
  p_node = (InternalPage *)page_buffer;

  if (!is_valid) {  // node deleted || outdated cache entry in cached node
    // invalidate the old node cache
    if (from_cache) {
      if(find_error) printf("invalidate! 554\n");
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }

    // re-read node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
    from_cache = false;
    retry_flag = INVALID_NODE;
    if(find_error) printf("invalid inner node. node.depth: %d, depth: %d, p_ptr: %lu, rev_ptr: %lu\n", p_node->hdr.depth, depth, p_node->rev_ptr.val, p_ptr.val);
    goto next;
  }

  // 3.2 Check header
  hdr = p_node->hdr;
  if (from_cache && !type_correct) {  // invalidate the out dated node type
    if(find_error) printf("invalidate! 444\n");
    index_cache->invalidate(entry_ptr_ptr, entry_ptr);
  }
  if (depth == hdr.depth) {
    index_cache->add_to_cache(k, p_node, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)));
  }

  for (int i = 0; i < hdr.partial_len; ++ i) { // check the prefix
    if (get_partial(k, hdr.depth + i) != hdr.partial[i]) { // The prefix is not a complete match, requiring a split. Insert a new leaf node and new inner nodes
      // need split
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
      int partial_len = hdr.depth + i - depth;  // hdr.depth may be outdated, so use partial_len wrt. depth
      bool res = out_of_place_write_node(k, v, depth, partial_len, hdr.partial[i], p_ptr, p, node_ptr, cas_buffer, cxt, coro_id);
      // cas fail, retry
      if (!res) {
        if (from_cache) {
          index_cache->invalidate(entry_ptr_ptr, entry_ptr);
        }
        p = *(InternalEntry*) cas_buffer; // p has expired, use teh returned value of CAS to update p
        from_cache = false;
        retry_flag = CAS_LEAF;
        if(find_error) printf("concurrent conflict 592\n");
        goto next;
      }

      // invalidate cache node due to outdated cache entry in cache node
      if (from_cache) {
        if(find_error) printf("invalidate! 470\n");
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }

      // modify the header of the current inner node; the type field, depth, prefix length, and prefix array all need to be changed
      auto header_buffer = (dsm->get_rbuf(coro_id)).get_header_buffer();
      auto new_hdr = Header::split_header(hdr, i);
      dsm->cas_mask(GADD(p.addr(), sizeof(GlobalAddress)), (uint64_t)hdr, (uint64_t)new_hdr, header_buffer, ~Header::node_type_mask, false, cxt);
      if(find_error) printf("stop here 606\n");
      goto insert_finish;
    }
  }
  // If the prefix is a complete match, then check if there is an InternalEntry with the same prefix. 
  // If found, it requires traversing down; if not found, search for an empty slot to insert a new leaf node
  depth = hdr.depth + hdr.partial_len;
  node_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header));

  // 3.3 try get the next internalEntry
  max_num = node_type_to_num(p.type()); // Note, here we use 'p,' which is the latest and has had its node type verified
  if (depth < define::keyLen) {

            // search a exists slot first
            for (int i = 0; i < max_num; ++ i) {
              auto old_e = p_node->records[i];
              if (old_e != InternalEntry::Null() && old_e.partial == get_partial(k, depth)) { // traversing down
                entry_idx = i;
                p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + entry_idx * sizeof(InternalEntry));
                node_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)); // update node_ptr
                p = old_e;
                depth ++;
                from_cache = false;                
                retry_flag = FIND_NEXT;
                if(find_error) printf("search next! 632\n");
                goto next;  // search next level
              }
            }

            // if no match slot, then find an empty slot to insert leaf directly, no InternalEntry with the same prefix, search for an empty slot to insert the new leaf
            for (int i = 0; i < max_num; ++ i) {
              auto old_e = p_node->records[i];
              if (old_e == InternalEntry::Null()) {
                auto e_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
                auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

                auto new_leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
                auto new_leaf = new (new_leaf_buffer) Leaf(GlobalAddress::Null()); // fill rev_ptr in the 'out_of_place_write_leaf'         
                Key max_range_of_k = k;
                Key min_range_of_k = k;
                max_range_of_k.back() = 255; // the max value of uint8_t is 255
                min_range_of_k.back() = 0; // the min value of uint8_t is 0
                new_leaf->max = max_range_of_k;
                new_leaf->min = min_range_of_k;
                new_leaf->keys[0] = k;
                new_leaf->values[0] = v;
                auto new_leaf_addr = dsm->alloc(define::kLeafPageSize, false);

                bool res = out_of_place_write_leaf(new_leaf_buffer, new_leaf_addr, get_partial(min_range_of_k, depth), e_ptr, old_e, cas_buffer, cxt, coro_id);
                // cas success, return
                if (res) {
                  if (from_cache) {
                    index_cache->invalidate(entry_ptr_ptr, entry_ptr);
                  }
                  if(find_error) printf("stop here 662\n");
                  goto insert_finish;
                }
                // cas fail, check
                else { // the target slot is occupied during concurrent operations
                  auto e = *(InternalEntry*) cas_buffer;
                  if (e.partial == get_partial(k, depth)) {  // same partial keys insert to the same empty slot，if the prefix in 'InternalEntry' matches，traversing down
                    dsm->free(new_leaf_addr, define::kLeafPageSize);
                    entry_idx = i;
                    p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + entry_idx * sizeof(InternalEntry));
                    node_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)); // update node_ptr
                    p = e;
                    depth ++;
                    from_cache = false;                  
                    retry_flag = CAS_EMPTY;
                    if(find_error) printf("retry cas! 677\n");
                    goto next;  // search next level
                  } // if the prefixes are different, continue the for loop to find the next empty slot
                }
              }
            } // there is no empty slot, to expand node type. in any case, the node reserves 256 slots, which is sufficient

            // 3.4 node is full, switch node type
            int slot_id;
            cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            if (insert_behind(k, v, depth + 1, get_partial(k, depth), p.type(), node_ptr, cas_buffer, slot_id, cxt, coro_id)){  // insert success
              auto next_type = num_to_node_type(slot_id);
              cas_node_type(next_type, p_ptr, p, hdr, cxt, coro_id);

              if (from_cache) {  // cache is outdated since node type is changed
                if(find_error) printf("invalidate! 556\n");
                index_cache->invalidate(entry_ptr_ptr, entry_ptr);
              }
              if(find_error) printf("stop here 570\n");
              goto insert_finish;
            } else { // find an InternalEntry with the same prefix in slots beyond max_num (due to the cache becoming outdated), then continue traversing down
              entry_idx = slot_id;
              p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + entry_idx * sizeof(InternalEntry));
              node_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)); // update node_ptr
              p = *(InternalEntry*) cas_buffer;
              depth ++;
              from_cache = false;              
              retry_flag = INSERT_BEHIND_EMPTY;
              if(find_error) printf("search next! 705\n");
              goto next;
            }
  } else { // special inner node
          uint8_t target = 0;
          uint8_t pos = 0;
          bool find = false;
          bool update_node_type = false;
          int num_of_slot;
          for (int i = 0; i < 16; ++ i) { // the number of entries in the special inner node is limited to 16
            if (p_node->records[i].val == 0) {
              num_of_slot = i;
              break;
            }
            const auto& e = p_node->records[i];
            if (target == 0 && e.partial == target) { // the prefix is 0
              pos = i;
              find = true;
            } else if (e.partial <= k.at(define::keyLen-1) && e.partial > target) {
              target = e.partial;
              pos = i;
              find = true;
              if (i >= max_num) update_node_type = true; // this possibility does not exist; the code needs to be cleaned up
            }
          }
          assert(find);
          if (update_node_type) {
            assert(false);
            auto next_type = num_to_node_type(num_of_slot);
            cas_node_type(next_type, p_ptr, p, hdr, cxt, coro_id);
          }
          entry_idx = pos;
          p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + entry_idx * sizeof(InternalEntry));
          node_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)); // update node_ptr
          p = p_node->records[entry_idx];
          depth ++;
          from_cache = false;        
          retry_flag = FIND_NEXT;
          goto next;

  }

insert_finish:
  if (!write_handover) {
    auto hit = (cache_depth == 1 ? 0 : (double)cache_depth / depth);
    cache_hit[dsm->getMyThreadID()] += hit;
    cache_miss[dsm->getMyThreadID()] += (1 - hit);
  }
  local_lock_table->release_local_write_lock(k, lock_res);
  return;
}

// Read leaf node
bool Tree::read_leaf(const GlobalAddress &leaf_addr, char *leaf_buffer, const GlobalAddress &p_ptr, bool from_cache, CoroContext *cxt, int coro_id) {
  try_read_leaf[dsm->getMyThreadID()] ++;
re_read:
  dsm->read_sync(leaf_buffer, leaf_addr, define::kLeafPageSize, cxt);
  auto leaf = (Leaf *)leaf_buffer;
  // udpate reverse pointer if needed
  if (!from_cache && leaf->rev_ptr != p_ptr) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(GADD(leaf_addr, rev_ptr_offset), leaf->rev_ptr, p_ptr, cas_buffer, false, cxt);
  }
  // invalidation
  if (!leaf->is_valid(p_ptr, from_cache)) {
    leaf_cache_invalid[dsm->getMyThreadID()] ++;
    return false;
  }
  if (!leaf->is_consistent()) {
    read_leaf_retry[dsm->getMyThreadID()] ++;
    goto re_read;
  }
  return true;
}

// Faa Ternary State node lock and read newest leaf node
bool Tree::faa_and_read(const GlobalAddress &leaf_addr, char *leaf_buffer, const GlobalAddress &p_ptr, bool from_cache, uint64_t *faa_buffer, CoroContext *cxt, int coro_id) {
  try_read_leaf[dsm->getMyThreadID()] ++;
  dsm->faa(GADD(leaf_addr, lock_offset), define::normalOp, faa_buffer, true, cxt);
  dsm->read(leaf_buffer, leaf_addr, define::kLeafPageSize, true, cxt);
  if(cxt == nullptr) {
    dsm->poll_rdma_cq(2);
  }

  auto leaf = (Leaf *)leaf_buffer;

  // udpate reverse pointer if needed
  if (!from_cache && leaf->rev_ptr != p_ptr) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(GADD(leaf_addr, rev_ptr_offset), leaf->rev_ptr, p_ptr, cas_buffer, false, cxt);
  }
  // invalidation
  if (!leaf->is_valid(p_ptr, from_cache)) {
    return false;
  }
  return true;
}

// Create the root node of Dolphin
bool Tree::write_root_node(const Key &k, Value &v, uint8_t partial_key, const GlobalAddress &e_ptr, 
                            const InternalEntry &old_e, uint64_t *ret_buffer, CoroContext *cxt, int coro_id) 
{
  // allocate & write
  auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
  new (leaf_buffer) Leaf(e_ptr);

  Key max_range_of_k = k;
  Key min_range_of_k = k;
  max_range_of_k.back() = 255; // the max value of uint8_t is 255
  min_range_of_k.back() = 0; // the min value of uint8_t is 0
  Leaf *new_leaf = (Leaf *) leaf_buffer;
  new_leaf->max = max_range_of_k;
  new_leaf->min = min_range_of_k;
  new_leaf->keys[0] = k;
  new_leaf->values[0] = v;

  auto leaf_addr = dsm->alloc(define::kLeafPageSize, false);

  dsm->write_sync(leaf_buffer, leaf_addr, define::kLeafPageSize, cxt);

  // cas entry
  auto new_e = InternalEntry(partial_key, leaf_addr);
  auto remote_cas = [=](){
    return dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
  };

  return remote_cas();
}

// Insert a pointer into the empty InternalEntry pointed to by e_ptr, pointing to a new leaf node
bool Tree::out_of_place_write_leaf(char* new_leaf_buffer, GlobalAddress& leaf_addr, uint8_t partial_key,
                                   const GlobalAddress &e_ptr, const InternalEntry &old_e, uint64_t *ret_buffer, CoroContext *cxt, int coro_id) {

  auto new_leaf = (Leaf *)new_leaf_buffer;
  new_leaf->rev_ptr = e_ptr;
  dsm->write_sync(new_leaf_buffer, leaf_addr, define::kLeafPageSize, cxt);

  // cas entry
  auto new_e = InternalEntry(partial_key, leaf_addr);
  auto remote_cas = [=](){
    return dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
  };

  return remote_cas();
}


// Read inner node
bool Tree::read_node(InternalEntry &p, bool& type_correct, char *node_buffer, const GlobalAddress& p_ptr, int depth, bool from_cache,
                     CoroContext *cxt, int coro_id) {
  auto read_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(p.type()) * sizeof(InternalEntry);
  dsm->read_sync(node_buffer, p.addr(), read_size, cxt);
  auto p_node = (InternalPage *)node_buffer;
  auto& hdr = p_node->hdr;

  read_node_type[dsm->getMyThreadID()][hdr.type()] ++;
  try_read_node[dsm->getMyThreadID()] ++;

  // Only handle errors with mismatched node types; other errors require reiteration.
  // Therefore, return to the main function for further processing upon completion of this function
  if (hdr.node_type != p.node_type) {
    if (hdr.node_type > p.node_type) {  // need to read the rest part
      read_node_repair[dsm->getMyThreadID()] ++;
      auto remain_size = (node_type_to_num(hdr.type()) - node_type_to_num(p.type())) * sizeof(InternalEntry);
      dsm->read_sync(node_buffer + read_size, GADD(p.addr(), read_size), remain_size, cxt);
    }
    p.node_type = hdr.node_type;
    type_correct = false;
  }
  type_correct = true;
  // udpate reverse pointer if needed
  if (!from_cache && p_node->rev_ptr != p_ptr) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(p.addr(), p_node->rev_ptr, p_ptr, cas_buffer, false, cxt);
    // dsm->cas_sync(p.addr(), p_node->rev_ptr, p_ptr, cas_buffer, cxt);
  }
  return p_node->is_valid(p_ptr, depth, from_cache);
}

// Create a new leaf node and corresponding inner nodes
bool Tree::out_of_place_write_node(const Key &k, Value &v, int depth, int partial_len, uint8_t diff_partial,
                                   const GlobalAddress &e_ptr, const InternalEntry &old_e, const GlobalAddress& node_addr,
                                   uint64_t *ret_buffer, CoroContext *cxt, int coro_id) {
  int new_node_num = partial_len / (define::hPartialLenMax + 1) + 1;

  // allocate node
  GlobalAddress *node_addrs = new GlobalAddress[new_node_num];
  dsm->alloc_nodes(new_node_num, node_addrs); // If it fails, the inner node space allocated here also needs to be released

  // allocate & write new leaf
  auto new_leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
  auto leaf_e_ptr = GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header) + sizeof(InternalEntry) * 1);

  new (new_leaf_buffer) Leaf(leaf_e_ptr);
  auto new_leaf_addr = dsm->alloc(define::kLeafPageSize, false); // If it fails, the leaf node space allocated here also needs to be released

  Key max_range_of_k = k;
  Key min_range_of_k = k;
  max_range_of_k.back() = 255; // the max value of uint8_t is 255
  min_range_of_k.back() = 0; // the min value of uint8_t is 0
  Leaf *new_leaf = (Leaf *) new_leaf_buffer;
  new_leaf->max = max_range_of_k;
  new_leaf->min = min_range_of_k;
  new_leaf->keys[0] = k;
  new_leaf->values[0] = v;

  // init inner nodes
  NodeType nodes_type = num_to_node_type(15);
  InternalPage ** node_pages = new InternalPage* [new_node_num];
  auto rev_ptr = e_ptr;
  for (int i = 0; i < new_node_num - 1; ++ i) {
    auto node_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    node_pages[i] = new (node_buffer) InternalPage(k, define::hPartialLenMax, depth, nodes_type, rev_ptr);
    node_pages[i]->records[0] = InternalEntry(get_partial(k, depth + define::hPartialLenMax),
                                              nodes_type, node_addrs[i + 1]);
    rev_ptr = GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header));
    partial_len -= define::hPartialLenMax + 1;
    depth += define::hPartialLenMax + 1;
  }

  // insert the two leaf(inner node) into the last node
  auto node_buffer  = (dsm->get_rbuf(coro_id)).get_page_buffer();
  node_pages[new_node_num - 1] = new (node_buffer) InternalPage(k, partial_len, depth, nodes_type, rev_ptr);
  node_pages[new_node_num - 1]->records[0] = InternalEntry(diff_partial, old_e);
  node_pages[new_node_num - 1]->records[1] = InternalEntry(get_partial(k, depth + partial_len), new_leaf_addr);

  // init the parent entry
  auto new_e = InternalEntry(old_e.partial, nodes_type, node_addrs[0]);
  auto page_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(nodes_type) * sizeof(InternalEntry);

  // batch_write nodes (doorbell batching)
  int i;
  RdmaOpRegion *rs =  new RdmaOpRegion[new_node_num + 1];
  for (i = 0; i < new_node_num; ++ i) {
    rs[i].source     = (uint64_t)node_pages[i];
    rs[i].dest       = node_addrs[i];
    rs[i].size       = page_size;
    rs[i].is_on_chip = false;
  }
  rs[new_node_num].source     = (uint64_t)new_leaf_buffer;
  rs[new_node_num].dest       = new_leaf_addr;
  rs[new_node_num].size       = define::kLeafPageSize;
  rs[new_node_num].is_on_chip = false;
  dsm->write_batches_sync(rs, new_node_num + 1, cxt, coro_id);

  // cas
  auto remote_cas = [=](){
    return dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
  };
  auto reclaim_memory = [=](){
    for (int i = 0; i < new_node_num; ++ i) {
      dsm->free(node_addrs[i], define::allocAlignPageSize);
    }
    dsm->free(new_leaf_addr, define::kLeafPageSize);
  };

  bool res = remote_cas();
  if (!res) reclaim_memory();

  // cas the updated rev_ptr inside old leaf node / old inner node
  if (res) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(GADD(old_e.addr(), rev_ptr_offset), e_ptr, GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header)), cas_buffer, false, cxt);
  }


  if (res) {
    for (int i = 0; i < new_node_num; ++ i) {
      index_cache->add_to_cache(k, node_pages[i], GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header)));
    }
  }

  // free
  delete[] rs; delete[] node_pages; delete[] node_addrs;
  return res;
}

// 
bool Tree::out_of_place_write_node_with_two_leaf(const Key &k, char* new_leaf_buffer, char* old_leaf_buffer, GlobalAddress old_leaf_addr, 
                                   int depth, int partial_len, uint8_t diff_partial, const GlobalAddress &e_ptr, const InternalEntry &old_e, 
                                   const GlobalAddress& node_addr, uint64_t *ret_buffer, CoroContext *cxt, int coro_id) {
  int new_node_num = partial_len / (define::hPartialLenMax + 1) + 1;
  // allocate node
  GlobalAddress *node_addrs = new GlobalAddress[new_node_num];
  for (int i = 0; i < new_node_num-1; ++ i) {
    node_addrs[i] = dsm->alloc(define::allocationPageSize, true);
  }
  node_addrs[new_node_num-1] = dsm->alloc(8 + 8 + 16 * 8, true);


  // allocate & write new leaf
  auto new_leaf_addr =  dsm->alloc(define::kLeafPageSize, false); // new leaf node
  auto new_leaf = (Leaf *)new_leaf_buffer;
  auto rev_ptr_tmp = GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header) + sizeof(InternalEntry) * 1);
  new_leaf->rev_ptr = rev_ptr_tmp;

  // modify rev_ptr of old leaf node
  auto old_leaf = (Leaf *)old_leaf_buffer;
  rev_ptr_tmp = GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header));
  old_leaf->rev_ptr = rev_ptr_tmp;

  // init inner nodes
  NodeType nodes_type = num_to_node_type(15);
  InternalPage ** node_pages = new InternalPage* [new_node_num];
  auto rev_ptr = e_ptr;
  for (int i = 0; i < new_node_num - 1; ++ i) {
    auto node_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    node_pages[i] = new (node_buffer) InternalPage(k, define::hPartialLenMax, depth, nodes_type, rev_ptr);
    node_pages[i]->records[0] = InternalEntry(get_partial(k, depth + define::hPartialLenMax),
                                              nodes_type, node_addrs[i + 1]);
    rev_ptr = GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header));
    partial_len -= define::hPartialLenMax + 1;
    depth += define::hPartialLenMax + 1;
  }

  // insert the two leaf into the last inner node
  auto node_buffer  = (dsm->get_rbuf(coro_id)).get_page_buffer();
  node_pages[new_node_num - 1] = new (node_buffer) InternalPage(k, partial_len, depth, nodes_type, rev_ptr);
  node_pages[new_node_num - 1]->records[0] = InternalEntry(diff_partial, old_e);
  node_pages[new_node_num - 1]->records[1] = InternalEntry(get_partial(k, depth + partial_len), new_leaf_addr);

  // init the parent entry
  auto new_e = InternalEntry(old_e.partial, nodes_type, node_addrs[0]);
  auto page_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(nodes_type) * sizeof(InternalEntry);

  // batch_write nodes (doorbell batching)
  int i;
  RdmaOpRegion *rs =  new RdmaOpRegion[new_node_num + 1];
  for (i = 0; i < new_node_num; ++ i) {
    rs[i].source     = (uint64_t)node_pages[i];
    rs[i].dest       = node_addrs[i];
    rs[i].size       = page_size;
    rs[i].is_on_chip = false;
  }
  rs[new_node_num].source     = (uint64_t)new_leaf_buffer;
  rs[new_node_num].dest       = new_leaf_addr;
  rs[new_node_num].size       = define::kLeafPageSize;
  rs[new_node_num].is_on_chip = false;

  dsm->write_batches_sync(rs, new_node_num + 1, cxt, coro_id);

  // cas
  auto remote_cas = [=](){
    return dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
  };
  auto reclaim_memory = [=](){
    for (int i = 0; i < new_node_num-1; ++ i) {
      dsm->free(node_addrs[i], define::allocAlignPageSize);
    }
    dsm->free(node_addrs[new_node_num-1], 8+8+16*8);
    dsm->free(new_leaf_addr, define::kLeafPageSize);
  };

  bool res = remote_cas();
  if (!res) reclaim_memory();

  // update the old leaf node
  if (res) {
    dsm->write_sync(old_leaf_buffer, old_leaf_addr, lock_offset, cxt);
  }

  if (res) {
    for (int i = 0; i < new_node_num; ++ i) {
      index_cache->add_to_cache(k, node_pages[i], GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header)));
    }
  }

  // free
  delete[] rs; delete[] node_pages; delete[] node_addrs;
  return res;
}

// update the type of ART inner node
void Tree::cas_node_type(NodeType next_type, GlobalAddress p_ptr, InternalEntry p, Header hdr,
                         CoroContext *cxt, int coro_id) {
  auto node_addr = p.addr();
  auto header_addr = GADD(node_addr, sizeof(GlobalAddress));
  auto cas_buffer_1 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  auto cas_buffer_2 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
  std::pair<bool, bool> res = std::make_pair(false, false);

  // batch cas old_entry & node header to change node type
  auto remote_cas_both = [=, &p_ptr, &p, &hdr](){
    auto new_e = InternalEntry(next_type, p);
    RdmaOpRegion rs[2];
    rs[0].source     = (uint64_t)cas_buffer_1;
    rs[0].dest       = p_ptr;
    rs[0].is_on_chip = false;
    rs[1].source     = (uint64_t)cas_buffer_2;
    rs[1].dest       = header_addr;
    rs[1].is_on_chip = false;
    return dsm->two_cas_mask_sync(rs[0], (uint64_t)p, (uint64_t)new_e, ~0UL,
                                  rs[1], hdr, Header(next_type), Header::node_type_mask, cxt);
  };

  // only cas old_entry
  auto remote_cas_entry = [=, &p_ptr, &p](){
    auto new_e = InternalEntry(next_type, p);
    return dsm->cas_sync(p_ptr, (uint64_t)p, (uint64_t)new_e, cas_buffer_1, cxt);
  };

  // only cas node_header
  auto remote_cas_header = [=, &hdr](){
    return dsm->cas_mask_sync(header_addr, hdr, Header(next_type), cas_buffer_2, Header::node_type_mask, cxt);
  };

  // read down to find target entry when split
  auto read_first_entry = [=, &p_ptr, &p](){
    p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header));
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
  };

re_switch:
  auto old_res = res;
  if (!old_res.first && !old_res.second) {
    res = remote_cas_both();
  }
  else {
    if (!old_res.first)  res.first  = remote_cas_entry();
    if (!old_res.second) res.second = remote_cas_header();
  }
  if (!res.first) {
    p = *(InternalEntry *)cas_buffer_1;
    // handle the conflict when switch & split/delete happen at the same time
    while (p != InternalEntry::Null() && !p.is_leaf && p.addr() != node_addr) {
      read_first_entry();
      retry_cnt[dsm->getMyThreadID()][SWITCH_FIND_TARGET] ++;
    }
    if (p.addr() != node_addr || p.type() >= next_type) res.first = true;  // no need to retry
  }
  if (!res.second) {
    hdr = *(Header *)cas_buffer_2;
    if (hdr.type() >= next_type) res.second = true;  // no need to retry
  }
  if (!res.first || !res.second) {
    retry_cnt[dsm->getMyThreadID()][SWITCH_RETRY] ++;
    goto re_switch;
  }
}

// The inner node is full, switch the node type, and then insert the new leaf node to the back of inner node
bool Tree::insert_behind(const Key &k, Value &v, int depth, uint8_t partial_key, NodeType node_type,
                         const GlobalAddress &node_addr, uint64_t *ret_buffer, int& inserted_idx,
                         CoroContext *cxt, int coro_id) {
  int max_num, i;
  assert(node_type != NODE_256);
  max_num = node_type_to_num(node_type);

  auto new_leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
  auto new_leaf = new (new_leaf_buffer) Leaf(GlobalAddress::Null()); // fill in rev_ptr in the out_of_place_write_leaf()        
  Key max_range_of_k = k;
  Key min_range_of_k = k;
  max_range_of_k.back() = 255; // the max value of uint8_t is 255
  min_range_of_k.back() = 0; // the min value of uint8_t is 0
  new_leaf->max = max_range_of_k;
  new_leaf->min = min_range_of_k;
  new_leaf->keys[0] = k;
  new_leaf->values[0] = v;
  auto new_leaf_addr = dsm->alloc(define::kLeafPageSize, false);

  // try cas an empty slot in the inner node
  for (i = 0; i < 256 - max_num; ++ i) {
    auto slot_id = max_num + i;
    GlobalAddress e_ptr = GADD(node_addr, slot_id * sizeof(InternalEntry)); // empty slot
    bool res = out_of_place_write_leaf(new_leaf_buffer, new_leaf_addr, partial_key, e_ptr, InternalEntry::Null(), ret_buffer, cxt, coro_id);
    // cas success, return to switch node type
    if (res) {
      inserted_idx = slot_id;
      return true;
    }
    // cas fail, check
    else {
      auto e = *(InternalEntry*) ret_buffer;
      if (e.partial == partial_key) {  // same partial keys insert to the same empty slot
        dsm->free(new_leaf_addr, define::kLeafPageSize);
        inserted_idx = slot_id;
        return false;  // search next level
      }
    }
    retry_cnt[dsm->getMyThreadID()][INSERT_BEHIND_TRY_NEXT] ++;
  }
  assert(false);
}

// Get target kv
bool Tree::search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  // handover
  bool search_res = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);
  bool read_handover = false;

  // traversal
  GlobalAddress p_ptr;
  InternalEntry p;
  GlobalAddress node_ptr;  // node address(excluding header)
  int depth;
  int retry_flag = FIRST_TRY;

  // cache
  bool from_cache = false;
  volatile CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  int entry_idx = -1;
  int cache_depth = 0;

  // temp
  char* page_buffer;
  bool is_valid, type_correct;
  InternalPage* p_node = nullptr;
  Header hdr;
  int max_num;

  lock_res = local_lock_table->acquire_local_read_lock(k, &busy_waiting_queue, cxt, coro_id);
  read_handover = (lock_res.first && !lock_res.second);
  try_read_op[dsm->getMyThreadID()]++;
  if (read_handover) {
    read_handover_num[dsm->getMyThreadID()]++;
    goto search_finish;
  }

  // search local cache
  from_cache = index_cache->search_from_cache(k, entry_ptr_ptr, entry_ptr, entry_idx);
  if (from_cache) { // cache hit
    assert(entry_idx >= 0);
    p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
    p = entry_ptr->records[entry_idx];
    node_ptr = entry_ptr->addr;
    depth = entry_ptr->depth;
  }
  else {
    p_ptr = root_ptr_ptr;
    p = get_root_ptr(cxt, coro_id);
    depth = 0;
  }

  depth ++;
  cache_depth = depth;
  assert(p != InternalEntry::Null());

next:
  retry_cnt[dsm->getMyThreadID()][retry_flag] ++;

  // 1. If we are at a NULL node, search finished
  if (p == InternalEntry::Null()) {
    assert(from_cache == false);
    search_res = false;
    goto search_finish;
  }

  // 2. If we are at a leaf, read the leaf
  if (p.is_leaf) {
    // 2.1 read the leaf
    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
    is_valid = read_leaf(p.addr(), leaf_buffer, p_ptr, from_cache, cxt, coro_id);

    if (!is_valid) {
      // invalidate the old cache entry
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }

      // re-read leaf entry
      auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
      dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
      p = *(InternalEntry *)entry_buffer;
      from_cache = false;
      retry_flag = INVALID_LEAF;
      goto next;
    }
    auto leaf = (Leaf *)leaf_buffer;
    auto min = leaf->min;
    auto max = leaf->max;
    bool upper = (leaf->max.back() == 255) ? true : false;

    if ((k >= min) && (k < max || upper)) { // if k is within the range of current leaf node
      // 2.2 search target key
      for (int i = 0; i < kLeafCardinality; ++i) {
        Key rkey = leaf->keys[i];
        if (rkey == k) { // find target key!
            v = leaf->values[i];
            search_res = true;
            goto search_finish;
        }
        if (key2int(rkey) == 0) { // leaf node is not full, but target key does not esist.
          search_res = false;
          goto search_finish;
        }
      }
      // leaf node is full, but target key does not esist. 
      search_res = false;
      goto search_finish;
    } else if (k < min) {
      search_res = false;
      goto search_finish;
    } else if (k >= max && !upper) { // parent special inner node / cache is outdated, reread parent inner node and get the latest target leaf node 
        if (from_cache) {
          index_cache->invalidate(entry_ptr_ptr, entry_ptr);
        }
        page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
        dsm->read_sync(page_buffer, GSUB(node_ptr, sizeof(GlobalAddress) + sizeof(Header)), define::allocationPageSize - sizeof(InternalEntry)*240, cxt); // 后续衡量一下读取的size
        p_node = (InternalPage *)page_buffer;
        if (depth-1 == p_node->hdr.depth + p_node->hdr.partial_len) {
          index_cache->add_to_cache(k, p_node, node_ptr);
        }

        uint8_t target = 0;
        uint8_t pos = 0;
        bool find = false;
        for (int i = 0; i < 16; ++ i) { // the number of entries in the special inner node is limited to 16
          if (p_node->records[i].val == 0) {
            break;
          }
          const auto& e = p_node->records[i];
          if (target == 0 && e.partial == target) { // if partial == target == 0
            pos = i;
            find = true;
          } else if (e.partial <= k.at(define::keyLen-1) && e.partial > target) {
            target = e.partial;
            pos = i;
            find = true;
          }
        }
        assert(find);

        entry_idx = pos;
        p_ptr = GADD(node_ptr, sizeof(InternalEntry) * entry_idx);
        p = p_node->records[entry_idx];
        from_cache = false;
        retry_flag = INVALID_LEAF;
        goto next;
    }

  }

  // 3. Find out an inner node
  // 3.1 read the inner node
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node(p, type_correct, page_buffer, p_ptr, depth, from_cache, cxt, coro_id);
  p_node = (InternalPage *)page_buffer;

  if (!is_valid) {  // node deleted || outdated cache entry in cached node
    // invalidate the old node cache
    if (from_cache) {
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
    // re-read inner node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
    from_cache = false;
    retry_flag = INVALID_NODE;
    goto next;
  }

  // 3.2 Check header
  hdr = p_node->hdr;
  if (from_cache && !type_correct) {  // invalidate the out dated node type
    index_cache->invalidate(entry_ptr_ptr, entry_ptr);
  }
  if (depth == hdr.depth) {
    index_cache->add_to_cache(k, p_node, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)));
  }

  for (int i = 0; i < hdr.partial_len; ++ i) {
    if (get_partial(k, hdr.depth + i) != hdr.partial[i]) {
      search_res = false;
      goto search_finish;
    }
  }
  depth = hdr.depth + hdr.partial_len;

  // 3.3 try get the next internalEntry
  max_num = node_type_to_num(p.type());
  // find from the exist slot
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e != InternalEntry::Null() && old_e.partial == get_partial(k, hdr.depth + hdr.partial_len)) {
      p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      node_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)); // update node_ptr
      p = old_e;
      from_cache = false;
      depth ++;
      retry_flag = FIND_NEXT;
      goto next;  // search next level
    }
  }

search_finish:
  if (!read_handover) {
    auto hit = (cache_depth == 1 ? 0 : (double)cache_depth / depth);
    cache_hit[dsm->getMyThreadID()] += hit;
    cache_miss[dsm->getMyThreadID()] += (1 - hit);
  }

  local_lock_table->release_local_read_lock(k, lock_res, search_res, v);  // handover the ret leaf addr
  return search_res;
}


void Tree::search_entries(const Key &from, const Key &to, int target_depth, std::vector<ScanContext> &res, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  GlobalAddress p_ptr;
  InternalEntry p;
  int depth;
  bool from_cache = false;
  volatile CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  int entry_idx = -1;
  int cache_depth = 0;

  bool type_correct;
  char* page_buffer;
  bool is_valid;
  InternalPage* p_node;
  Header hdr;
  int max_num;

  // search local cache
  from_cache = index_cache->search_from_cache(from, entry_ptr_ptr, entry_ptr, entry_idx);
  if (from_cache) { // cache hit
    assert(entry_idx >= 0);
    p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
    p = entry_ptr->records[entry_idx];
    depth = entry_ptr->depth;
  }
  else {
    p_ptr = root_ptr_ptr;
    p = get_root_ptr(cxt, coro_id);
    depth = 0;
  }

  depth ++;
  cache_depth = depth;

next:
  // 1. If we are at a NULL node
  if (p == InternalEntry::Null()) {
    goto search_finish;
  }

  // 2. Check if it is the target depth
  if (depth == target_depth) {
    res.push_back(ScanContext(p, p_ptr, depth-1, from_cache, entry_ptr_ptr, entry_ptr, from, to, BORDER, BORDER));
    goto search_finish;
  }
  if (p.is_leaf) {
    goto search_finish;
  }

  // 3. Find out an inner node
  // 3.1 read the inner node
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node(p, type_correct, page_buffer, p_ptr, depth, from_cache, cxt, coro_id);
  p_node = (InternalPage *)page_buffer;

  if (!is_valid) {  // node deleted || outdated cache entry in cached node
    // invalidate the old node cache
    if (from_cache) {
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
    // re-read node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
    from_cache = false;
    goto next;
  }

  // 3.2 Check header
  hdr = p_node->hdr;
  if (from_cache && !type_correct) {
    index_cache->invalidate(entry_ptr_ptr, entry_ptr);  // invalidate the out dated node type
  }

  for (int i = 0; i < hdr.partial_len; ++ i) {
    if (get_partial(from, hdr.depth + i) != hdr.partial[i]) {
      goto search_finish;
    }
    if (hdr.depth + i + 1 == target_depth) {
      range_query_on_page(p_node, from_cache, depth-1,
                          p_ptr, p,
                          from, to, BORDER, BORDER, res);
      goto search_finish;
    }
  }
  depth = hdr.depth + hdr.partial_len;

  // 3.3 try get the next internalEntry
  // find from the exist slot
  max_num = node_type_to_num(p.type());
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e != InternalEntry::Null() && old_e.partial == get_partial(from, hdr.depth + hdr.partial_len)) {
      p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      p = old_e;
      from_cache = false;
      depth ++;
      goto next;  // search next level
    }
  }
search_finish:
  auto hit = (cache_depth == 1 ? 0 : (double)cache_depth / depth);
  cache_hit[dsm->getMyThreadID()] += hit;
  cache_miss[dsm->getMyThreadID()] += (1 - hit);

  return;
}

/*
  range query, DO NOT support corotine currently
*/
// [from, to)
void Tree::range_query(const Key &from, const Key &to, std::map<Key, Value> &ret) {
  thread_local std::vector<ScanContext> survivors;
  thread_local std::vector<RdmaOpRegion> rs;
  thread_local std::vector<ScanContext> si;
  thread_local std::vector<RangeCache> range_cache;
  thread_local std::set<uint64_t> tokens;

  assert(dsm->is_register());
  if (to <= from) return;

  range_cache.clear();
  tokens.clear();

  auto range_buffer = (dsm->get_rbuf(0)).get_range_buffer();
  int cnt;

  // search local cache
  index_cache->search_range_from_cache(from, to, range_cache);
  // entries in cache
  for (auto & rc : range_cache) {
    survivors.push_back(ScanContext(rc.e, rc.e_ptr, rc.depth, true, rc.entry_ptr_ptr, rc.entry_ptr,
                                    std::max(rc.from, from),
                                    std::min(rc.to, to - 1),
                                    rc.from <= from   ? BORDER : INSIDE,   // TODO: outside?
                                    rc.to   >= to - 1 ? BORDER : INSIDE));
  }
  if (range_cache.empty()) {
    int partial_len = longest_common_prefix(from, to - 1, 0);
    search_entries(from, to - 1, partial_len, survivors, nullptr, 0);
  }

  int idx = 0;
next_level:
  idx  ++;
  if (survivors.empty()) {  // exit
    return;
  }
  rs.clear();
  si.clear();

  // 1. batch read the current level of nodes / leaves
  cnt = 0;
  for(auto & s : survivors) {
    auto& p = s.e;
    auto token = (uint64_t)p.addr();
    if (tokens.find(token) == tokens.end()) {
      RdmaOpRegion r;
      r.source     = (uint64_t)range_buffer + cnt * define::allocationPageSize;
      r.dest       = p.addr();
      r.size       = p.is_leaf ? define::kLeafPageSize : (
                              s.from_cache ?  // TODO: art
                              (sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(NODE_256) * sizeof(InternalEntry)) :
                              (sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(p.type()) * sizeof(InternalEntry))
                          );
      r.is_on_chip = false;
      rs.push_back(r);
      si.push_back(s);
      cnt ++;
      tokens.insert(token);
    }
  }
  survivors.clear();
  // printf("cnt=%d\n", cnt);

  // 2. separate requests with its target node, and read them using doorbell batching for each batch
  dsm->read_batches_sync(rs);

  // 3. process the read nodes and leaves
  for (int i = 0; i < cnt; ++ i) {
    // 3.1 if it is leaf, check & save result
    if (si[i].e.is_leaf) {
      Leaf *leaf = (Leaf *)(range_buffer + i * define::allocationPageSize);
      auto k = leaf->keys[0];

      if (!leaf->is_valid(si[i].e_ptr, si[i].from_cache)) {
        // invalidate the old leaf entry cache
        if (si[i].from_cache) {
          index_cache->invalidate(si[i].entry_ptr_ptr, si[i].entry_ptr);
        }

        // re-read leaf entry
        auto entry_buffer = (dsm->get_rbuf(0)).get_entry_buffer();
        dsm->read_sync((char *)entry_buffer, si[i].e_ptr, sizeof(InternalEntry));
        si[i].e = *(InternalEntry *)entry_buffer;
        si[i].from_cache = false;
        survivors.push_back(si[i]);
        continue;
      }
      if (!leaf->is_consistent()) {  // re-read leaf is unconsistent
        survivors.push_back(si[i]);
      }

      if (k >= from && k < to) {  // [from, to)
        ret[k] = leaf->values[0];
        // TODO: cache hit ratio
      }
    }
    // 3.2 if it is node, check & choose in-range entry in it
    else {
      InternalPage* node = (InternalPage *)(range_buffer + i * define::allocationPageSize);
      if (!node->is_valid(si[i].e_ptr, si[i].depth + 1, si[i].from_cache)) {  // node deleted || outdated cache entry in cached node
        // invalidate the old node cache
        if (si[i].from_cache) {
          index_cache->invalidate(si[i].entry_ptr_ptr, si[i].entry_ptr);
        }
        // re-read node entry
        auto entry_buffer = (dsm->get_rbuf(0)).get_entry_buffer();
        dsm->read_sync((char *)entry_buffer, si[i].e_ptr, sizeof(InternalEntry));
        si[i].e = *(InternalEntry *)entry_buffer;
        si[i].from_cache = false;
        survivors.push_back(si[i]);
        continue;
      }
      range_query_on_page(node, si[i].from_cache, si[i].depth,
                          si[i].e_ptr, si[i].e,
                          si[i].from, si[i].to, si[i].l_state, si[i].r_state, survivors);
    }
  }
  goto next_level;
}

// get target entries in current inner node
void Tree::range_query_on_page(InternalPage* page, bool from_cache, int depth,
                               GlobalAddress p_ptr, InternalEntry p,
                               const Key &from, const Key &to, State l_state, State r_state,
                               std::vector<ScanContext>& res) {
  // check header
  auto& hdr = page->hdr;
  // assert(ei.depth + 1 == hdr.depth);  // only in condition of no concurrent insert
  if (depth == hdr.depth - 1) {
    index_cache->add_to_cache(from, page, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)));
  }

  if (l_state == BORDER) { // left state: BORDER --> other state
    int j;
    for (j = 0; j < hdr.partial_len; ++ j) if (hdr.partial[j] != get_partial(from, hdr.depth + j)) break;
    if (j == hdr.partial_len) l_state = BORDER;
    else if (hdr.partial[j] > get_partial(from, hdr.depth + j)) l_state = INSIDE;
    else l_state = OUTSIDE;
  }
  if (r_state == BORDER) {  // right state: BORDER --> other state
    int j;
    for (j = 0; j < hdr.partial_len; ++ j) if (hdr.partial[j] != get_partial(to, hdr.depth + j)) break;
    if (j == hdr.partial_len) r_state = BORDER;
    else if (hdr.partial[j] < get_partial(to, hdr.depth + j)) r_state = INSIDE;
    else r_state = OUTSIDE;
  }
  if (l_state == OUTSIDE || r_state == OUTSIDE) return;

  // check partial & choose entry from records
  const uint8_t from_partial = get_partial(from, hdr.depth + hdr.partial_len);
  const uint8_t to_partial   = get_partial(to  , hdr.depth + hdr.partial_len);
  int max_num = node_type_to_num(hdr.type());
  for(int j = 0; j < max_num; ++ j) {
    const auto& e = page->records[j];
    if (e == InternalEntry::Null()) continue;

    auto e_l_state = l_state;
    auto e_r_state = r_state;

    if (e_l_state == BORDER)  {  // left state: BORDER --> other state
      if (e.partial == from_partial) e_l_state = BORDER;
      else if (e.partial > from_partial) e_l_state = INSIDE;
      else e_l_state = OUTSIDE;
    }
    if (e_r_state == BORDER){    // right state: BORDER --> other state
      if (e.partial == to_partial) e_r_state = BORDER;
      else if (e.partial < to_partial) e_r_state = INSIDE;
      else e_r_state = OUTSIDE;
    }
    if (e_l_state != OUTSIDE && e_r_state != OUTSIDE) {
      auto next_from = from;
      auto next_to = to;
      // calculate [from, to) for this survivor entry
      if (e_l_state == INSIDE) {
        for (int i = 0; i < hdr.partial_len; ++ i) next_from = remake_prefix(next_from, hdr.depth + i, hdr.partial[i]);
        next_from = remake_prefix(next_from, hdr.depth + hdr.partial_len, e.partial);
      }
      if (e_r_state == INSIDE) {
        for (int i = 0; i < hdr.partial_len; ++ i) next_to   = remake_prefix(next_to  , hdr.depth + i, hdr.partial[i]);
        next_to   = remake_prefix(next_to  , hdr.depth + hdr.partial_len, e.partial);
      }
      res.push_back(ScanContext(e, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + j * sizeof(InternalEntry)),
                                hdr.depth + hdr.partial_len, false, nullptr, nullptr, next_from, next_to, e_l_state, e_r_state));
    }
  }
}


void Tree::run_coroutine(GenFunc gen_func, WorkFunc work_func, int coro_cnt, Request* req, int req_num) {
  using namespace std::placeholders;

  assert(coro_cnt <= MAX_CORO_NUM);
  for (int i = 0; i < coro_cnt; ++i) {
    RequstGen *gen = gen_func(dsm, req, req_num, i, coro_cnt);
    worker[i] = CoroCall(std::bind(&Tree::coro_worker, this, _1, gen, work_func, i));
  }

  master = CoroCall(std::bind(&Tree::coro_master, this, _1, coro_cnt));

  master();
}


void Tree::coro_worker(CoroYield &yield, RequstGen *gen, WorkFunc work_func, int coro_id) {
  CoroContext ctx;
  ctx.coro_id = coro_id;
  ctx.master = &master;
  ctx.yield = &yield;

  Timer coro_timer;
  auto thread_id = dsm->getMyThreadID();

  while (!need_stop) {
    auto r = gen->next();

    coro_timer.begin();
    work_func(this, r, &ctx, coro_id);
    auto us_10 = coro_timer.end() / 100;

    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[thread_id][coro_id][us_10]++;
  }
}


void Tree::coro_master(CoroYield &yield, int coro_cnt) {
  for (int i = 0; i < coro_cnt; ++i) {
    yield(worker[i]);
  }
  while (!need_stop) {
    uint64_t next_coro_id;

    if (dsm->poll_rdma_cq_once(next_coro_id)) {
      yield(worker[next_coro_id]);
    }
    // uint64_t wr_ids[POLL_CQ_MAX_CNT_ONCE];
    // int cnt = dsm->poll_rdma_cq_batch_once(wr_ids, POLL_CQ_MAX_CNT_ONCE);
    // for (int i = 0; i < cnt; ++ i) {
    //   yield(worker[wr_ids[i]]);
    // }

    if (!busy_waiting_queue.empty()) {
    // int cnt = busy_waiting_queue.size();
    // while (cnt --) {
      auto next = busy_waiting_queue.front();
      busy_waiting_queue.pop();
      next_coro_id = next.first;
      if (next.second()) {
        yield(worker[next_coro_id]);
      }
      else {
        busy_waiting_queue.push(next);
      }
    }
  }
}


void Tree::statistics() {
  index_cache->statistics();
}

void Tree::clear_debug_info() {
  memset(cache_miss, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(cache_hit, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(lock_fail, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  // memset(try_lock, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(write_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_write_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_leaf_retry, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(leaf_cache_invalid, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_leaf, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_node_repair, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_node, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_node_type, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_NODE_TYPE_NUM);
  memset(retry_cnt, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_FLAG_NUM);
}
