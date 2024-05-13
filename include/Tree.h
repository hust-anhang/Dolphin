#if !defined(_TREE_H_)
#define _TREE_H_

#include "RadixCache.h"
#include "DSM.h"
#include "Common.h"
#include "LocalLockTable.h"

#include <atomic>
#include <city.h>
#include <functional>
#include <map>
#include <algorithm>
#include <queue>
#include <set>
#include <iostream>


/*
  Workloads
*/
struct Request {
  bool is_search;
  bool is_insert;
  bool is_update;
  Key k;
  Value v;
  int range_size;
};


class RequstGen {
public:
  RequstGen() = default;
  virtual Request next() { return Request{}; }
};


/*
  Tree
*/
using GenFunc = std::function<RequstGen *(DSM*, Request*, int, int, int)>;
#define MAX_FLAG_NUM 12
enum {
  FIRST_TRY,
  CAS_NULL,
  INVALID_LEAF,
  CAS_LEAF,
  INVALID_NODE,
  SPLIT_HEADER,
  FIND_NEXT,
  CAS_EMPTY,
  INSERT_BEHIND_EMPTY,
  INSERT_BEHIND_TRY_NEXT,
  SWITCH_RETRY,
  SWITCH_FIND_TARGET,
};

class Tree {
public:
  Tree(DSM *dsm, uint16_t tree_id = 0);

  using WorkFunc = std::function<void (Tree *, const Request&, CoroContext *, int)>;
  void run_coroutine(GenFunc gen_func, WorkFunc work_func, int coro_cnt, Request* req = nullptr, int req_num = 0);

  void insert(const Key &k, Value v, CoroContext *cxt = nullptr, int coro_id = 0, bool is_update = false, bool is_load = false);
  bool search(const Key &k, Value &v, CoroContext *cxt = nullptr, int coro_id = 0);
  void range_query(const Key &from, const Key &to, std::map<Key, Value> &ret);
  void statistics();
  void clear_debug_info();

  GlobalAddress get_root_ptr_ptr();
  InternalEntry get_root_ptr(CoroContext *cxt, int coro_id);

private:
  void coro_worker(CoroYield &yield, RequstGen *gen, WorkFunc work_func, int coro_id);
  void coro_master(CoroYield &yield, int coro_cnt);

  bool read_leaf(const GlobalAddress &leaf_addr, char *leaf_buffer, const GlobalAddress &p_ptr, bool from_cache, CoroContext *cxt, int coro_id);

  bool out_of_place_write_leaf(char* leaf_buffer, GlobalAddress& leaf_addr, uint8_t partial_key,
                               const GlobalAddress &e_ptr, const InternalEntry &old_e, uint64_t *ret_buffer,
                               CoroContext *cxt, int coro_id);

  bool read_node(InternalEntry &p, bool& type_correct, char *node_buffer, const GlobalAddress& p_ptr, int depth, bool from_cache,
                 CoroContext *cxt, int coro_id);
  bool out_of_place_write_node(const Key &k, Value &v, int depth, int partial_len, uint8_t diff_partial,
                               const GlobalAddress &e_ptr, const InternalEntry &old_e, const GlobalAddress& node_addr, uint64_t *ret_buffer,
                               CoroContext *cxt, int coro_id);

  bool insert_behind(const Key &k, Value &v, int depth, uint8_t partial_key, NodeType node_type,
                     const GlobalAddress &node_addr, uint64_t *ret_buffer, int& inserted_idx,
                     CoroContext *cxt, int coro_id);
  void search_entries(const Key &from, const Key &to, int target_depth, std::vector<ScanContext> &res,
                      CoroContext *cxt, int coro_id);
  void cas_node_type(NodeType next_type, GlobalAddress p_ptr, InternalEntry p, Header hdr,
                     CoroContext *cxt, int coro_id);
  void range_query_on_page(InternalPage* page, bool from_cache, int depth,
                           GlobalAddress p_ptr, InternalEntry p,
                           const Key &from, const Key &to, State l_state, State r_state,
                           std::vector<ScanContext>& res);

  // ah added
  bool write_root_node(const Key &k, Value &v, uint8_t partial_key, const GlobalAddress &e_ptr, 
                            const InternalEntry &old_e, uint64_t *ret_buffer, CoroContext *cxt, int coro_id); 
  bool faa_and_read(const GlobalAddress &leaf_addr, char *leaf_buffer, const GlobalAddress &p_ptr, bool from_cache, uint64_t *faa_buffer, CoroContext *cxt, int coro_id);

  bool out_of_place_write_node_with_two_leaf(const Key &k, char* new_leaf_buffer, char* old_leaf_buffer, GlobalAddress old_leaf_addr, int depth,
                               int partial_len, uint8_t diff_partial, const GlobalAddress &e_ptr, const InternalEntry &old_e, 
                               const GlobalAddress& node_addr, uint64_t *ret_buffer, CoroContext *cxt, int coro_id);
  bool insert_a_leaf_to_inner_node(const Key &k, Value &v, int depth, uint8_t partial_key, NodeType node_type,
                         const GlobalAddress &node_addr, uint64_t *ret_buffer, int& inserted_idx,
                         CoroContext *cxt, int coro_id);

private:
  DSM *dsm;
  RadixCache *index_cache;
  LocalLockTable *local_lock_table;

  static thread_local CoroCall worker[MAX_CORO_NUM];
  static thread_local CoroCall master;
  static thread_local CoroQueue busy_waiting_queue;

  uint64_t tree_id;
  GlobalAddress root_ptr_ptr; // the address which stores root pointer;
};


#endif // _TREE_H_
