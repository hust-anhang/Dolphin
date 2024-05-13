#if !defined(_NODE_H_)
#define _NODE_H_

#include "Common.h"
#include "GlobalAddress.h"
#include "Key.h"

#define TREE_ENABLE_FINE_GRAIN_NODE

struct PackedGAddr {  // 48-bit, used by node addr/leaf addr (not entry addr)
  uint64_t mn_id     : define::mnIdBit;
  uint64_t offset    : define::offsetBit;

  operator uint64_t() { return (offset << define::mnIdBit) | mn_id; }
} __attribute__((packed));


static_assert(sizeof(PackedGAddr) == 6);

constexpr int kLeafCardinality =
    (define::kLeafPageSize - sizeof(uint64_t)*2 - sizeof(Key) * 2 - sizeof(GlobalAddress) - sizeof(uint8_t)) /
    (sizeof(Key)+sizeof(Value));

constexpr int head_offset = sizeof(uint64_t) * 2 + sizeof(Key) * 2 + sizeof(GlobalAddress);
constexpr int rev_ptr_offset = 0;
constexpr int lock_offset = sizeof(uint64_t) * 2 + sizeof(Key) * 2 + kLeafCardinality * (sizeof(Key)+sizeof(Value));
/*
  Leaf Node, todo: when sizeof(Value) > 64Byte, apply CacheLine Version 
*/
class Leaf {
public:
  GlobalAddress rev_ptr;
  uint8_t front_version;
  uint8_t valid_byte;
  uint32_t node_id = 0;
  uint16_t last_index;
  Key min;
  Key max;

  Key keys[kLeafCardinality];
  Value values[kLeafCardinality];
  uint64_t lock;
  uint8_t rear_version;


public:
  Leaf() {}
  Leaf(const GlobalAddress& rev_ptr) : rev_ptr(rev_ptr), valid_byte(1), lock(0) {
    for (int i = 0; i < kLeafCardinality; i++){
      keys[i] = int2key(0);
      values[i] = 0;
    }
    front_version = 0;
    rear_version = 0;
    // sibling_ptr = GlobalAddress::Null();
    last_index = -1;
  }

  bool is_valid(const GlobalAddress& p_ptr, bool from_cache) const { return p_ptr == rev_ptr; }
  bool is_consistent() const {
    bool succ = true;
    succ = succ && (rear_version == front_version);
    return succ;
  }

  void set_consistent() {
    front_version++;
    rear_version = front_version;
  }

} __attribute__((packed));


/*
  Header
*/
#ifdef TREE_ENABLE_FINE_GRAIN_NODE
#define MAX_NODE_TYPE_NUM 8
enum NodeType : uint8_t {
  NODE_DELETED,
  NODE_4,
  NODE_8,
  NODE_16,
  NODE_32,
  NODE_64,
  NODE_128,
  NODE_256
};
#else
#define MAX_NODE_TYPE_NUM 5
enum NodeType : uint8_t {
  NODE_DELETED,
  NODE_4,
  NODE_16,
  NODE_48,
  NODE_256
};
#endif


inline int node_type_to_num(NodeType type) {
  if (type == NODE_DELETED) return 0;
// #ifndef TREE_ENABLE_ART
//   type = NODE_256;
// #endif
#ifdef TREE_ENABLE_FINE_GRAIN_NODE
  return 1 << (static_cast<int>(type) + 1);
#else
  switch (type) {
    case NODE_4  : return 4;
    case NODE_16 : return 16;
    case NODE_48 : return 48;
    case NODE_256: return 256;
    default:  assert(false);
  }
#endif
}


inline NodeType num_to_node_type(int num) {
  if (num == 0) return NODE_DELETED;
// #ifndef TREE_ENABLE_ART
//   return NODE_256;
// #endif
#ifdef TREE_ENABLE_FINE_GRAIN_NODE
  for (int i = 1; i < MAX_NODE_TYPE_NUM; ++ i) {
    if (num < (1 << (i + 1))) return static_cast<NodeType>(i);
  }
#else
  if (num < 4) return NODE_4;
  if (num < 16) return NODE_16;
  if (num < 48) return NODE_48;
  if (num < 256) return NODE_256;
#endif
  assert(false);
}


class Header {
public:
  union {
  struct {
    uint8_t depth;
    uint8_t node_type   : define::nodeTypeNumBit;
    uint8_t partial_len : 8 - define::nodeTypeNumBit;
    uint8_t partial[define::hPartialLenMax];
  };

  uint64_t val;
  };

public:
  Header() : depth(0), node_type(0), partial_len(0) { memset(partial, 0, sizeof(uint8_t) * define::hPartialLenMax); }
  Header(int depth) : depth(depth), node_type(0), partial_len(0) { memset(partial, 0, sizeof(uint8_t) * define::hPartialLenMax); }
  Header(NodeType node_type) : depth(0), node_type(node_type), partial_len(0) { memset(partial, 0, sizeof(uint8_t) * define::hPartialLenMax); }
  Header(const Key &k, int partial_len, int depth, NodeType node_type) : depth(depth), node_type(node_type), partial_len(partial_len) {
    assert((uint32_t)partial_len <= define::hPartialLenMax);
    for (int i = 0; i < partial_len; ++ i) partial[i] = get_partial(k, depth + i);
  }

  operator uint64_t() { return val; }

  bool is_match(const Key& k) {
    for (int i = 0; i < partial_len; ++ i) {
      if (get_partial(k, depth + i) != partial[i]) return false;
    }
    return true;
  }

  static Header split_header(const Header& old_hdr, int diff_idx) {
    auto new_hdr = Header();
    for (int i = diff_idx + 1; i < old_hdr.partial_len; ++ i) new_hdr.partial[i - diff_idx - 1] = old_hdr.partial[i];
    new_hdr.partial_len = old_hdr.partial_len - diff_idx - 1;
    new_hdr.depth = old_hdr.depth + diff_idx + 1;
    return new_hdr;
  }

  NodeType type() const {
    return static_cast<NodeType>(node_type);
  }
  static const uint64_t node_type_mask = (((1UL << define::nodeTypeNumBit) - 1) << 8);
} __attribute__((packed));


static_assert(sizeof(Header) == 8);
static_assert(1UL << (8 - define::nodeTypeNumBit) >= define::hPartialLenMax);


/*
  Internal Nodes
*/
class InternalEntry {
public:
  union {
  union {
    // is_leaf = 0
    struct {
      uint8_t  partial;

      uint8_t  mn_id     : define::kvLenBit - define::nodeTypeNumBit;
      uint8_t  node_type : define::nodeTypeNumBit;

      uint8_t  is_leaf   : 1;
      uint64_t next_addr : 48;
    }__attribute__((packed));

    // is_leaf = 1
    struct {
      uint8_t  _partial;
      uint8_t  _mn_id     : define::kvLenBit;
      uint8_t  _is_leaf   : 1;
      uint64_t _next_addr : 48;
    }__attribute__((packed));
  };

  uint64_t val;
  };

public:
  InternalEntry() : val(0) {}
  InternalEntry(uint8_t partial, const GlobalAddress &addr) :
                _partial(partial), _mn_id(addr.nodeID), _is_leaf(1), _next_addr(addr.offset) {}
  InternalEntry(uint8_t partial, NodeType node_type, const GlobalAddress &addr) :
                partial(partial), mn_id(addr.nodeID), node_type(static_cast<uint8_t>(node_type)), is_leaf(0), next_addr(addr.offset) {}
  InternalEntry(uint8_t partial, const InternalEntry& e) :
                _partial(partial), _mn_id(e._mn_id), _is_leaf(e._is_leaf), _next_addr(e._next_addr) {}
  InternalEntry(NodeType node_type, const InternalEntry& e) :
                partial(e.partial), mn_id(e.mn_id), node_type(static_cast<uint8_t>(node_type)), is_leaf(e.is_leaf), next_addr(e._next_addr) {}

  operator uint64_t() const { return val; }

  static InternalEntry Null() {
    static InternalEntry zero;
    return zero;
  }

  NodeType type() const {
    return static_cast<NodeType>(node_type);
  }

  GlobalAddress addr() const {
    return GlobalAddress{mn_id, next_addr};
  }
} __attribute__((packed));

inline bool operator==(const InternalEntry &lhs, const InternalEntry &rhs) { return lhs.val == rhs.val; }
inline bool operator!=(const InternalEntry &lhs, const InternalEntry &rhs) { return lhs.val != rhs.val; }

static_assert(sizeof(InternalEntry) == 8);


class InternalPage {
public:
  // for invalidation
  GlobalAddress rev_ptr;

  Header hdr;
  InternalEntry records[256];

public:
  InternalPage() { std::fill(records, records + 256, InternalEntry::Null()); }
  InternalPage(const Key &k, int partial_len, int depth, NodeType node_type, const GlobalAddress& rev_ptr) : rev_ptr(rev_ptr), hdr(k, partial_len, depth, node_type) {
    std::fill(records, records + 256, InternalEntry::Null());
  }

  bool is_valid(const GlobalAddress& p_ptr, int depth, bool from_cache) const { return hdr.type() != NODE_DELETED && hdr.depth <= depth && (p_ptr == rev_ptr); }
} __attribute__((packed));


static_assert(sizeof(InternalPage) == 8 + 8 + 256 * 8);


/*
  Range Query
*/
enum State : uint8_t {
  INSIDE,
  BORDER,
  OUTSIDE
};

struct CacheEntry {
  // fixed
  uint8_t depth;
  GlobalAddress addr;
  std::vector<InternalEntry> records;
  // faa
  // volatile mutable uint64_t counter;

  CacheEntry() {}
  CacheEntry(const InternalPage* p_node, const GlobalAddress& addr) :
             depth(p_node->hdr.depth + p_node->hdr.partial_len), addr(addr) {
    for (int i = 0; i < node_type_to_num(p_node->hdr.type()); ++ i) {
      const auto& e = p_node->records[i];
      records.push_back(e);
    }
  }

  uint64_t content_size() const {
    return sizeof(uint8_t) + sizeof(GlobalAddress) + sizeof(InternalEntry) * records.size();  // + sizeof(uint64_t)
  }
};

using CacheKey = std::vector<uint8_t>;

struct RangeCache {
  Key from;
  Key to;  // include
  GlobalAddress e_ptr;
  InternalEntry e;
  int depth;
  volatile CacheEntry** entry_ptr_ptr;
  CacheEntry* entry_ptr;
  RangeCache() {}
  RangeCache(const Key& from, const Key& to, const GlobalAddress& e_ptr, const InternalEntry& e, int depth, volatile CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr) :
             from(from), to(to), e_ptr(e_ptr), e(e), depth(depth), entry_ptr_ptr(entry_ptr_ptr), entry_ptr(entry_ptr) {}
};


struct ScanContext {
  InternalEntry e;
  GlobalAddress e_ptr;
  int depth;
  bool from_cache;
  volatile CacheEntry** entry_ptr_ptr;
  CacheEntry* entry_ptr;
  Key from;
  Key to;  // include
  State l_state;
  State r_state;
  ScanContext() {}
  ScanContext(const InternalEntry& e, const GlobalAddress& e_ptr, int depth, bool from_cache, volatile CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr,
              const Key& from, const Key& to, State l_state, State r_state) :
              e(e), e_ptr(e_ptr), depth(depth), from_cache(from_cache), entry_ptr_ptr(entry_ptr_ptr), entry_ptr(entry_ptr),
              from(from), to(to), l_state(l_state), r_state(r_state) {}
};

#endif // _NODE_H_
