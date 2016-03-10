//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// BWTree.h
//
// Identification: src/backend/index/BWTree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <memory>
#include <vector>
#include <atomic>
#include <bits/atomic_base.h>
#include <backend/common/types.h>
#include <ostream>
#include <cstdint>
#include <thread>
#include <cassert>
#include <time.h>

#include "index.h"

// MASALA<libcuckoo/city_hasher.hh>
MASALA<cuckoo/default_hasher.hh>
MASALA<cuckoo/cuckoohash_util.hh>
MASALA<cuckoo/cuckoohash_config.hh>
MASALA<cuckoo/cuckoohash_map.hh>

//Null page id
#define NULL_PID 0
#define TABLESIZE 10000

// turn on or off debug mode
//#define DEBUG

namespace peloton {
namespace index {

  // Look up the stx btree interface for background.
  // peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, class KeyComparator,
    class KeyEqualityChecker>
class BWTree {

  private:
    // logical pointer type
    typedef unsigned int pid_t;
    size_t memory_usage;

    // key value pair type
    typedef const std::pair<KeyType, ValueType>& KVType;

    typedef uint64_t epoch;

    //Delta and standard node types
    enum NodeType : std::int8_t {
      leaf,
      inner,
      indexDelta,
      deleteIndex,
      deltaSplitInner,
      mergeInner,
      deltaInsert,
      deltaDelete,
      deltaSplitLeaf,
      mergeLeaf,
      removeNode,
    };

    struct Node {
    private:
      // used for debug
      const char *node_names_[11] =
          {
              "leaf", "inner", "indexDelta", "deleteIndex", "deltaSplitInner",
              "mergeInner", "deltaInsert", "deltaDelete", "deltaSplitLeaf",
              "mergeLeaf", "removeNode"
          };

    protected:
      // type of this node
      NodeType type;

      // print helper
      //  virtual std::ostream& stream_write(std::ostream& ) const {}

    public:
      //ptr to the next node in delta chain
      Node* next;

      //used to track length of the chain till this node
      int chain_length;

      //used for split/merge case detection
      int record_count;

      int level;

      // used by non-leaf nodes to track
      // presence of index delta
      bool has_index_delta;

      // used by leaf nodes to track
      // the presence of split delta
      bool has_split_delta;

      bool is_consolidate_blocked;

      pid_t pid;

      virtual void set_next(Node *) {}

      inline bool is_leaf() {
        return (level == 0);
      }

      inline NodeType get_type() {
        return type;
      }

      friend std::ostream& operator<<(std::ostream& os, const Node& node) {
        return node.print_node(os);
      }

      inline std::ostream& print_node(std::ostream& os) const {
        return  os << "PID: " << pid << std::endl
                << "Type: " << node_names_[type] << std::endl
                << "Chain length: " << chain_length << std::endl
                << "Record count: " << record_count << std::endl
                << "Level: " << level << std::endl;
      }

      virtual ~Node() {}

    };

    std::atomic<Node*> gc_head_;

    // TODO: performance issues?
    struct LockFreeTable {
      // because internals of libcuckoo: reference/value, copy constructor stuff..
      // use pointers
      cuckoohash_map<int, std::atomic<Node*>*, DefaultHasher<int> > table;

      inline LockFreeTable()
      {}

      // lookup and return physical pointer corresponding to pid
      inline Node* get_phy_ptr(const pid_t& pid) {
        // return the node pointer
        return table.find(pid)->load(std::memory_order_acquire);
      }

      // Used for first time insertion of this pid into the map
      inline void insert_new_pid(const pid_t& pid, Node* node) {
        // insert into the map
        // std::atomic<T> isn't copy-constructible, nor copy-assignable.
        // only thread here, relaxed access
        std::atomic<Node*>* n = new std::atomic<Node*>();
        n->store(node, std::memory_order_relaxed);
        table.insert(pid, n);
        return;
      }

      //tries to install the updated phy ptr
      inline bool install_node(const pid_t& pid, Node* expected, Node* update) {

        // try to atomically update the new node
        return std::atomic_compare_exchange_weak_explicit(
            &(*(table.find(pid))), &expected, update,
            std::memory_order_release, std::memory_order_relaxed);
      }

      inline void erase(const pid_t& pid) {
        std::atomic<Node*>* n = table.find(pid);
        delete(n);
        table.erase(pid);
      }
    };

//    /**
//     * Epoch Table
//     * coarse granularity: Thread -> Epoch
//     * TODO better granularity: Thread -> (Epoch, PID)
//     * 0. handout pointer to epoch_entry, to save lookup, use linked list
//     * need to handle remove epoch_entry from list, only one thread will delete
//     * one epoch_entry, but many threads can trigger delete at the same time
//     * 1. use fixed size array, handout epoch. just free the spot.
//     * possibility of collision, with long running thread
//     * 2. use concurrent hashmap
//     *
//     * Delete Entry: head of delta chain to be deleted
//     * lock free linked list
//     */
//
//    class epoch_manager
//    {
//      struct delete_entry
//      {
//        Node* payload;
//        struct delete_entry *next;
//        delete_entry(Node *p, delete_entry* ent) {
//          payload = p;
//          next = ent;
//        }
//      };
//      struct epoch_entry
//      {
//        uint32_t refcnt;
//        epoch epoch_;
//        std::atomic<delete_entry*> list; // start nodes of deltachain to GC
//        // struct epoch_entry *next;
//        epoch_entry(uint32_t r, epoch e, delete_entry* ent) {
//          refcnt = r;
//          epoch_ = e;
//          list = ent;
//        }
//      };
//      std::atomic<epoch_entry*> epoch_table[TABLESIZE] = {};
//    private:
//      epoch_entry* nullptrlvalue = nullptr;
//      epoch *epoch_;
//      // ticker *tk;
//      struct timespec t = {0, 40*1000*1000}; // 40 ms
//
//      void deleteChain(Node *node) {
//        if (node == nullptr)
//          return;
//        Node * next = node->next;
//        while (next != nullptr) {
//          delete node;
//          node = next;
//          next = next->next;
//        }
//        delete node;
//      }
//      void GC(epoch_entry *entry) {
//        delete_entry * node = entry->list;
//        if (node == nullptr)
//          return;
//        delete_entry * next = node->next;
//        while (next != nullptr) {
//          deleteChain(node->payload);
//          delete node;
//          node = next;
//          next = next->next;
//        }
//        deleteChain(node->payload);
//        delete node;
//        delete(entry);
//      };
//      /**
//			 * ticker increments epoch
//			 * and check epoch update, and run GC on previous epoch
//			 */
//      void ticker() {
//        for(;;) {
//          nanosleep(&t, nullptr);
//          epoch previous = *epoch_;
//          (*epoch_)++;
//          epoch_entry *entry = epoch_table[previous % TABLESIZE].load(std::memory_order_relaxed);
//          if (entry != nullptr && entry->refcnt == 0) {
//            assert(entry->epoch_ == previous);
//            bool ret = std::atomic_compare_exchange_weak_explicit(
//                &epoch_table[previous % TABLESIZE], &entry, nullptrlvalue,
//                std::memory_order_release, std::memory_order_relaxed);
//            // someone else succeeds
//            if (!ret)
//              return;
//            std::thread thd(&epoch_manager::GC, this, entry);
//            thd.detach();
//          }
//        }
//      };
//    public:
//      epoch_manager() {
//        epoch_ = new epoch(0UL);
//        std::thread thd(&epoch_manager::ticker, this);
//        thd.detach();
//      };
//      ~epoch_manager() {
//        delete(epoch_);
//      };
//      // called by normal threads
//      epoch enter() {
//        epoch current = *epoch_;
//        epoch_entry *entry = epoch_table[current % TABLESIZE].load(std::memory_order_relaxed);
//        if (entry != nullptr) {
//          assert(entry->epoch_ == current); // make sure no collision
//          entry->refcnt++;
//        } else {
//          entry = new epoch_entry(1, current, nullptr);
//          bool ret = std::atomic_compare_exchange_weak_explicit(
//              &epoch_table[current % TABLESIZE], &nullptrlvalue, entry,
//              std::memory_order_release, std::memory_order_relaxed);
//          // someone else succeeds
//          if (!ret) {
//            delete(entry);
//            epoch_entry *entry = epoch_table[current % TABLESIZE].load(std::memory_order_relaxed);
//            assert(entry->epoch_ == current); // make sure no collision
//            entry->refcnt++;
//          }
//        }
//        return current;
//      };
//      void exit(epoch e) {
//        // when refcnt drop to zero, and epoch has progressed, spawn thread to do GC
//        epoch current = *epoch_;
//        epoch_entry *entry = epoch_table[e % TABLESIZE].load(std::memory_order_relaxed);
//        assert(entry->epoch_ == e); // make sure no collision
//        // TODO can have leaked entries because no thread join before epoch moves on
//        if (--entry->refcnt == 0 && current > entry->epoch_) {
//          bool ret = std::atomic_compare_exchange_weak_explicit(
//              &epoch_table[e % TABLESIZE], &entry, nullptrlvalue,
//              std::memory_order_release, std::memory_order_relaxed);
//          // someone else succeeds
//          if (!ret)
//            return;
//          std::thread thd(&epoch_manager::GC, this, entry);
//          thd.detach();
//        }
//      };
//      // called by consolidate
//      void addGCEntry(epoch e, Node *node) {
//        epoch_entry *entry = epoch_table[e % TABLESIZE].load(std::memory_order_relaxed);
//        assert(entry->epoch_ == e); // make sure no collision
//        delete_entry *head, *nnew;
//        nnew = new delete_entry(node, nullptr);
//        // retry until succeed
//        do {
//          head = entry->list.load(std::memory_order_relaxed);
//          nnew->next = head;
//        } while (!std::atomic_compare_exchange_weak_explicit(
//            &entry->list, &head, nnew,
//            std::memory_order_release, std::memory_order_relaxed)
//            );
//      };
//    };

    class ItemPointerComparator {
    public:
      ItemPointerComparator(){};

      bool operator ()(const ValueType& v1, const ValueType& v2) const {
        auto p1 = static_cast<ItemPointer>(v1);
        auto p2 = static_cast<ItemPointer>(v2);
        // block and offset comparison ordering
        if (p1.block == p2.block){
          return (p1.offset < p2.offset);
        }
        return (p1.block < p2.block);
      }
    };

    // lock free mapping table
    LockFreeTable mapping_table_;

    // pid generator for nodes
    std::atomic_ushort pid_gen_;

    // logical pointer to root
    std::atomic<pid_t> root_;

    // for memory footprint
    std::atomic<size_t> memory_usage_;

    // comparator, assuming the default comparator is lt
    KeyComparator key_comparator_;

    // equality checker
    KeyEqualityChecker eq_checker_;

    // value comparator
    ItemPointerComparator val_comparator_;

    // Logical pointer to head leaf page
    pid_t head_leaf_ptr_;

    // Logical pointer to the tail leaf page
    pid_t tail_leaf_ptr_;

    // leaf consolidation threshold
    int consolidate_threshold_leaf_;

    // inner node consolidation threshold
    int consolidate_threshold_inner_;

    // threshold for triggering split
    int split_threshold_;

    // threshold for triggering merge
    int merge_threshold_;

    // A tree node has a vector of key templates
    // and corresponding value templates
    // and a side link
    template <typename K, typename V>
    struct TreeNode : public Node {
      // node's key vector
      std::vector<std::pair<K, V>> key_values;

      // logical pointer to next leaf on the right
      pid_t sidelink;

    };

    // leaf node of the bw-tree, has KeyType and corresp.
    // vector of ValueType of record
    struct LeafNode : public TreeNode<KeyType, std::vector<ValueType>> {
      //leaf nodes are always level 0
      inline LeafNode(const pid_t self, const pid_t nextleaf) {
        // doubly linked list of leaves
        this->sidelink = nextleaf;

        this->pid = self;

        this->type = NodeType::leaf;

        // block consolidate only explicitly
        this->is_consolidate_blocked = false;

        //initialize chain length
        this->chain_length = 1;

        // no records yet
        this->record_count = 0;

        // leaf node, level 0
        this->level = 0;

        this->has_split_delta = false;

        // leaf node is always at the end of a delta chain
        this->next = nullptr;
      }

      void set_next(Node *next_node) {
        this->next = next_node;
        this->chain_length = next_node->chain_length + 1;
      }

      ~LeafNode(){};

    };

    // inner node of the bwtree, stores pairs of keys and the
    // child on their left pointer
    struct InnerNode : public TreeNode<KeyType, pid_t> {
      // pointer to the (n+1)th child, for n keys
      pid_t last_child;

      // highest key stored at this inner node
      KeyType kmax;

      // if kmax is infinity
      bool is_kmax_inf;

      // sets the maximum key
      inline InnerNode(const pid_t self, int level,
                       const pid_t adj_node,
                       const pid_t last_child_pid,
                       const KeyType& kmax) {
        this->type = NodeType::inner;

        this->pid = self;

        // inner node is always at the end of a delta chain
        this->next = nullptr;

        // set the sidelink
        this->sidelink = adj_node;

        // intialize chain length
        this->chain_length = 1;

        // intialize the level
        this->level = level;

        // block consolidate only explicitly
        this->is_consolidate_blocked = false;

        // no records intially
        this->record_count = 0;

        this->last_child = last_child_pid;

        this->has_index_delta = false;

        this->has_split_delta = false;

        this->kmax = kmax;

        this->is_kmax_inf = false;
      }

      // sets the maximum key as infinity
      inline InnerNode(const pid_t self, int level,
                       const pid_t adj_node,
                       const pid_t last_child_pid) {
        this->type = NodeType::inner;

        this->pid = self;

        // inner node is always at the end of a delta chain
        this->next = nullptr;

        // set the sidelink
        this->sidelink = adj_node;

        // intialize chain length
        this->chain_length = 1;

        // intialize the level
        this->level = level;

        // no records intially
        this->record_count = 0;

        this->last_child = last_child_pid;

        this->has_index_delta = false;

        this->has_split_delta = false;

        this->is_kmax_inf = true;
      }

      void set_next(Node *next_node) {
        this->next = next_node;
        this->chain_length = next_node->chain_length + 1;
      }

      ~InnerNode(){};
    };

    // IndexDelta record of the delta chain
    struct IndexDelta : public Node {
      // low and high to specify key range
      KeyType low;
      KeyType high;

      // chack if high key is infinity
      bool is_high_inf;

      // shortcut pointer to the new node
      pid_t new_node;

      inline IndexDelta(const KeyType& low, const KeyType& high,
                        const pid_t new_node) {

        // set the node's type
        this->type = NodeType::indexDelta;

        //low and high key ranges
        this->low = low;
        this->high = high;

        this->is_high_inf = false;

        // add logical pointer to the provided new node
        this->new_node = new_node;

      }

      // used to set infinite key
      inline IndexDelta(const KeyType& low, const pid_t new_node) {

        // set the node's type
        this->type = NodeType::indexDelta;

        // low and high key ranges
        this->low = low;

        // sets the key as infinity
        this->is_high_inf = true;

        // add logical pointer to the provided new node
        this->new_node = new_node;

      }

      void set_next(Node *next_node) {
        // next node in the delta chain
        this->next = next_node;

        // set self pid
        this->pid = next_node->pid;

        // chain has grown
        this->chain_length = next_node->chain_length + 1;

        //copy
        this->is_consolidate_blocked =
            next_node->is_consolidate_blocked;

        // a new record is being added, increment
        this->record_count = next_node->record_count + 1;

        // copy next node's split delta state
        this->has_split_delta = next_node->has_split_delta;

        // level remains the same
        this->level = next_node->level;

        // just installed an index delta
        this->has_index_delta = true;
      }

      ~IndexDelta(){};

    };

    // DeleteIndex delta record for node merging
    struct DeleteIndex : public Node {
      // low and high to specify key range
      KeyType low;
      KeyType high;

      // shortcut pointer to the merged node
      pid_t merge_node;

      inline DeleteIndex(const KeyType& low, const KeyType& high,
                         const pid_t merge_node) {

        // set the node's type
        this->type = NodeType::deleteIndex;

        //low and high key ranges
        this->low = low;
        this->high = high;

        // update the node for merging
        this->merge_node = merge_node;
      }

      void set_next(Node *next_node) {
        // next node in the delta chain
        this->next = next_node;

        // set self pid
        this->pid = next_node->pid;

        // update split delta state
        this->has_split_delta = next_node->has_split_delta;

        //copy
        this->is_consolidate_blocked =
            next_node->is_consolidate_blocked;

        // chain has grown
        this->chain_length = next_node->chain_length + 1;

        // a record is being removed, decrement
        this->record_count = next_node->record_count - 1;

        // level remains the same
        this->level = next_node->level;

        this->has_index_delta = next_node->has_index_delta;
      }

      ~DeleteIndex(){};

    };

    // split record for inner node
    struct DeltaSplitInner : public Node {

    public:
      // split key for this node
      KeyType splitKey;

      // shortcut pointer to the new node
      pid_t new_node;


      inline DeltaSplitInner(const KeyType& splitKey, const pid_t new_node,
                             const int new_record_count) {
        // set the node's type
        this->type = NodeType::deltaSplitInner;

        // set the split key
        this->splitKey = splitKey;

        // set the new node
        this->new_node = new_node;

        // get the new record count from SplitPage
        this->record_count = new_record_count;
      }

      // update the next pointer and bookeeping
      void set_next(Node *next_node) {
        // next node in the delta chain
        this->next = next_node;

        //copy
        this->is_consolidate_blocked =
            next_node->is_consolidate_blocked;

        // set self pid
        this->pid = next_node->pid;

        // chain has grown
        this->chain_length = next_node->chain_length + 1;

        // level remains the same
        this->level = next_node->level;

        // copy next node's split delta state
        this->has_split_delta = next_node->has_split_delta;

        this->has_index_delta = next_node->has_index_delta;
      }

      ~DeltaSplitInner(){};

    };

    struct MergeInner : public Node {
      // split key for this node
      KeyType splitKey;

      // physical pointer to the node being delted
      InnerNode* deleting_node;

      inline MergeInner(const KeyType& splitKey, InnerNode* deleting_node,
                        const int new_record_count) {
        // set the type
        this->type = NodeType::mergeInner;

        // set the split key
        this->splitKey = splitKey;

        // set the deleting node
        this->deleting_node = deleting_node;

        // update the record count from MergePage
        this->record_count = new_record_count;
      }

      // update the next pointer and bookeeping
      void set_next(Node *next_node) {
        // next node in the delta chain
        this->next = next_node;

        // set self pid
        this->pid = next_node->pid;

        // copy next node's split delta state
        this->has_split_delta = next_node->has_split_delta;

        //copy
        this->is_consolidate_blocked =
            next_node->is_consolidate_blocked;

        // chain has grown
        this->chain_length = next_node->chain_length + 1;

        // level remains the same
        this->level = next_node->level;

        this->has_index_delta = next_node->has_index_delta;
      }

      ~MergeInner(){};

    };

    // delta insert record
    struct DeltaInsert : public Node {
      // insert key
      KeyType key;

      //insert value
      ValueType value;

      inline DeltaInsert(const KeyType &key, const ValueType& value) {
        // set the type
        this->type = NodeType::deltaInsert;

        // set the key, value and base node
        this->key = key;
        this->value = value;
      }

      void set_next(Node *next_node) {
        this->next = next_node;

        // set self pid
        this->pid = next_node->pid;

        // copy next node's split delta state
        this->has_split_delta = next_node->has_split_delta;

        //copy
        this->is_consolidate_blocked =
            next_node->is_consolidate_blocked;

        // chain has grown
        this->chain_length = next_node->chain_length + 1;

        // a new record is being added, increment
        this->record_count = next_node->record_count + 1;

        // level remains the same
        this->level = next_node->level;
      }

      ~DeltaInsert(){};
    };

    struct DeltaDelete : public Node {
      // delete key
      KeyType key;

      // delete value
      ValueType value;

      inline DeltaDelete(const KeyType &key, const ValueType &val) {
        // set the base node
        this->type = NodeType::deltaDelete;

        // set the key and value
        this->key = key;

        this->value = val;
      }

      // sets the next node in the delta chain
      void set_next(Node *next_node) {
        this->next = next_node;

        // set self pid
        this->pid = next_node->pid;

        // update split delta state
        this->has_split_delta = next_node->has_split_delta;

        //copy
        this->is_consolidate_blocked =
            next_node->is_consolidate_blocked;

        // chain has grown
        this->chain_length = next_node->chain_length + 1;

        // a record is being removed, decrement
        this->record_count = next_node->record_count - 1;

        // level remains the same
        this->level = next_node->level;
      }

      ~DeltaDelete(){};

    };

    struct DeltaSplitLeaf : public Node {
      // split key for this record
      KeyType splitKey;

      // logical pointer to the new child record
      pid_t new_child;

      inline DeltaSplitLeaf(const KeyType &key, const pid_t new_child,
                            const int new_record_count) {
        // set the type
        this->type = NodeType::deltaSplitLeaf;

        // set the split key
        this->splitKey = key;

        // set the new child for split
        this->new_child = new_child;

        // split delta has been added
        this->has_split_delta = true;

        // set the updated record count from SplitPage
        this->record_count = new_record_count;
      }

      // sets the next node in the delta chain
      void set_next(Node *next_node) {
        this->next = next_node;

        // set self pid
        this->pid = next_node->pid;

        //copy
        this->is_consolidate_blocked =
            next_node->is_consolidate_blocked;

        // chain has grown
        this->chain_length = next_node->chain_length + 1;

        // level remains the same
        this->level = next_node->level;
      }

      ~DeltaSplitLeaf(){};
    };

    struct MergeLeaf : public Node {
      // split key for this node
      KeyType splitKey;

      // physical pointer to the node being delted
      LeafNode* deleting_node;

      inline MergeLeaf(const KeyType& splitKey, LeafNode* deleting_node,
                       const int new_record_count) {
        // set the type
        this->type = NodeType::mergeLeaf;

        // set the split key
        this->splitKey = splitKey;

        // set the deleting node
        this->deleting_node = deleting_node;

        // update the record count from MergePage
        this->record_count = new_record_count;
      }

      // update the next pointer and bookeeping
      void set_next(Node *next_node) {
        // next node in the delta chain
        this->next = next_node;

        // copy from next node
        this->has_split_delta = next_node->has_split_delta;

        //copy
        this->is_consolidate_blocked =
            next_node->is_consolidate_blocked;

        // set self pid
        this->pid = next_node->pid;

        // chain has grown
        this->chain_length = next_node->chain_length + 1;

        // level remains the same
        this->level = next_node->level;
      }

      ~MergeLeaf(){};
    };

    // Remove node delta record for any node
    struct RemoveNode : public Node {

      inline RemoveNode() {
        // set the type
        this->type = NodeType::removeNode;
      }

      void set_next(Node *next_node) {
        // set the next node
        this->next = next_node;

        // set self pid
        this->pid = next_node->pid;

        // update split delta state
        this->has_split_delta = next_node->has_split_delta;

        //copy
        this->is_consolidate_blocked =
            next_node->is_consolidate_blocked;

        // the delta chain has grown
        this->chain_length = next_node->chain_length + 1;

        // record count remains the same
        this->record_count = next_node->record_count;

        // level remains the same
        this->level = next_node->level;

        this->has_index_delta = next_node->has_index_delta;
      }

      ~RemoveNode(){};
    };

    // stores the result of the operation
    struct TreeOpResult {
      // status of the operation
      bool status;

      // values returned, if any
      std::vector<ValueType> values;

      // piud of the node we came from
      pid_t pid;

      // validity of the value
      bool is_valid_value;

      bool has_split;

      bool has_merge;

      // used to inform split key to parent
      KeyType kp;
      // the upper bound

      // used to inform Kq to parent in case of index delta
      KeyType kq;

      bool is_kq_inf;

      // used to handle new inner node edge case (root split)
      bool is_new_inner_node = false;

      // pid of the node requiring split/merge
      pid_t split_merge_pid;

      inline TreeOpResult(__attribute__((__unused__))
                          const bool status = false) :
          is_valid_value(false),
          has_split(false),
          has_merge(false)
      {}
    };

    // used by child node to check if SMO has finished on parent
    struct TreeState {
      // pid of parent
      pid_t parent_pid;

      // check if the parent has an index delta node
      bool has_index_delta;

      // key range upper bound
      KeyType kq;

      // should kq be considered as infinity?
      bool is_kq_inf;

      // the split key
      KeyType kp;

      // should kp considered as negative infinity?
      bool is_kp_inf;
    };

    // stores the result of a consolidate operation
    struct ConsolidateResult {
      // consolidate passed or failed?
      bool status;

      // did the node split during consolidate
      bool has_split;

      // did the node split during merge
      bool has_merge;

      // split key
      KeyType kp;

      // used to handle new inner node edge case (root split)
      bool is_new_inner_node = false;

      // node created by split
      pid_t split_child_pid;

    };
    inline TreeOpResult get_update_success_result() {
      return TreeOpResult(true);
    }

    inline TreeOpResult get_failed_result() {
      return TreeOpResult();
    }

    inline TreeOpResult make_search_fail_result() {
      return TreeOpResult(true);
    }

    // TODO: used for range scans
    class RangeVector {
      //logical pointer to the current node
      pid_t currnode;

      //cursor position within currnode
      int currpos;

      //stores the records from the range scan
      std::vector<std::pair<KeyType, ValueType>> range_result;

      inline RangeVector
          (const pid_t currnode, int currpos) :
          currnode(currnode), currpos(currpos)
      {}
    };

    // Compares two keys and returns true if a <= b
    inline bool key_compare_lte(const KeyType &a, const KeyType &b) {
      return !key_comparator_(b, a);
    }

    // Compares two keys and returns true if a < b
    inline bool key_compare_lt(const KeyType &a, const KeyType &b) {
      return key_comparator_(a, b);
    }

    // Compares two keys and returns true if a = b
    inline bool key_compare_eq(const KeyType &a, const KeyType &b) {
      return eq_checker_(a, b);
    }

    inline bool key_val_compare_eq(KVType a, KVType b){
      // check if keys are equal
      if(key_compare_eq(a.first, b.first)){
        auto p1 = static_cast<ItemPointer>(a.second);
        auto p2 = static_cast<ItemPointer>(b.second);
        // block and offset comparison ordering
        if (p1.block == p2.block &&
            p1.offset == p2.offset) {
          return true;
        }
      }
      return false;

    }

    // compare two values for equality
    inline bool val_eq(const ValueType &a, const ValueType &b) {
      auto p1 = static_cast<ItemPointer>(a);
      auto p2 = static_cast<ItemPointer>(b);
      return (p1.block == p2.block && p1.offset == p2.offset);
    }

    // Available modes: Greater than equal to, Greater tham
    enum NodeSearchMode {
      GTE, GT
    };

    void add_to_gc_chain(Node *head){
      // get to the bottom of the delta chain
      Node *expected, *ptr = head;
      while(ptr->next){
        ptr = ptr->next;
      }

      // keep trying till added to list
      do{
        expected = gc_head_.load(std::memory_order_relaxed);
        ptr->next = expected;
      }while(!std::atomic_compare_exchange_weak_explicit(
          &(gc_head_), &expected, head,
          std::memory_order_release, std::memory_order_relaxed));
    }

    void clear_gc_chain(){
      Node *head = gc_head_.load(std::memory_order_relaxed);
      Node *next = nullptr;

      while(head){
        next = head->next;
        delete head;
        head = next;
      }
      head = next = nullptr;
    }

    // Performs a binary search on a tree node to find the position of
    // the key nearest to the search key, depending on the mode.
    // Returns the position of the child to the left of nearest greater key
    // and -1 for failed search
    template <typename K, typename V>
    inline unsigned long node_key_search(const TreeNode<K, V> *node,
                                         const KeyType& key);

    enum OperationType : int8_t {
      insert_op,
      delete_op,
      search_op,
    };

    bool search_deleted_kv(const std::vector<std::pair<KeyType, ValueType>>
                           deleted_KV, const std::pair<KeyType, ValueType>& kv);


    // Does a tree operation on inner node (head of it's delta chain)
    // with leaf node operation passed as a function pointer
    TreeOpResult do_tree_operation(Node* head, const KeyType& key,
                                   const ValueType& value,
                                   const OperationType op_type,
                                   const TreeState &state);

    // Wrapper for the above function that looks up pid from mapping table
    TreeOpResult do_tree_operation(const pid_t node_pid, const KeyType& key,
                                   const ValueType& value,
                                   const OperationType op_type,
                                   const TreeState &state);

    // Search leaf page and return the found value, if exists. Try SMOs /
    // Consolidation, if necessary
    TreeOpResult search_leaf_page(Node *head, const KeyType& key,
                                  const TreeState &state);

    // Wrapper for the above function
    TreeOpResult search_leaf_page(const pid_t pid, const KeyType& key,
                                  const TreeState &state);


    // Update the leaf delta chain during insert/delta. Try SMOs /
    // Consolidation, if necessary
    TreeOpResult update_leaf_delta_chain(const pid_t pid, const KeyType& key,
                                         const ValueType& value,
                                         const OperationType op_type,
                                         const TreeState &state);

    // Update the leaf delta chain during insert/delta. Try SMOs /
    // Consolidation, if necessary
    TreeOpResult update_leaf_delta_chain(Node *head, const pid_t pid,
                                         const KeyType& key,
                                         const ValueType& value,
                                         const OperationType op_type,
                                         const TreeState &state);

    void unblock_consolidate(const pid_t pid);

    // consolidation leaf node delta chain
    ConsolidateResult consolidate_leaf(Node *node);

    // consolidate inner node delta chain
    ConsolidateResult consolidate_inner(Node *node);

    // splitpage inner
    DeltaSplitInner* splitPageInner(InnerNode *node, const std::vector<std::pair<KeyType,pid_t>>& wholePairs,
                                    pid_t qPID_last_child);

    // splitpage leaf
    DeltaSplitLeaf* splitPageLeaf(LeafNode *node, const std::vector<std::pair<KeyType,std::vector<ValueType>>>& wholePairs);

    // Merge page operation for node underflows
    bool merge_page(pid_t pid_l, pid_t pid_r, pid_t pid_parent);

    void setSibling(Node* node,pid_t sideNode);
    pid_t getSibling(Node* node);
    bool splitPage(pid_t pPID,pid_t rPID,pid_t pParentPID);
    bool checkIfRemoveDelta(Node* head);

    std::vector<std::pair<KeyType, std::vector<ValueType>>> getToBeMovedPairsLeaf(Node* headNodeP);
    std::vector<std::pair<KeyType, pid_t>> getToBeMovedPairsInner(Node* headNodeP);

  public:

    //BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>();
    //by default, start the pid generator at 1, 0 is NULL page
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker> (
        IndexMetadata *metadata)
        : key_comparator_(metadata),
          eq_checker_(metadata) {
      pid_gen_ = NULL_PID + 1;
      root_.store(static_cast<pid_t>(pid_gen_++), std::memory_order_release);
      memory_usage_.store(static_cast<size_t>(4194304), std::memory_order_release);

      //insert the chain into the mapping table
      mapping_table_.insert_new_pid(root_.load(std::memory_order_relaxed), new LeafNode(root_, NULL_PID));
      //memory_usage_.store(memory_usage_.load(std::memory_order_relaxed)+ sizeof(newnode), std::memory_order_release);
      //update the leaf pointers
      head_leaf_ptr_ = root_;
      tail_leaf_ptr_ = root_;

      // TODO: decide values
      consolidate_threshold_inner_ = 5;

      consolidate_threshold_leaf_ = 8;

      merge_threshold_ = 0;

      split_threshold_ = 150;

      gc_head_.store(nullptr, std::memory_order_release);

    }

    //bool Insert(__attribute__((unused)) KeyType key,
    //__attribute__((unused)) ValueType value);
    bool Insert(const KeyType &key, const ValueType& value);
    std::vector<ValueType> Search(const KeyType& key);
    bool Delete(const KeyType &key, const ValueType &val);
    std::vector<ValueType> AllKeyScan();
    bool Cleanup();
    size_t GetMemoryFootprint();

  #ifdef DEBUG
    // print tree for debugging
    void print_tree(const pid_t& pid);
    // Print a node
    void print_node(Node *node);
  #endif
  };

}  // End index namespace
}  // End peloton namespace
