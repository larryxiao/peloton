//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.cpp
//
// Identification: src/backend/index/bwtree.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <backend/common/logger.h>
#include "backend/index/bwtree.h"
#include "index_key.h"
#include "bwtree.h"

#define TODO

namespace peloton {
namespace index {

  typedef unsigned int pid_t;

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker> template <typename K, typename V>
  unsigned long BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  node_key_search(const TreeNode<K, V> *node, const KeyType &key) {

    memory_usage = 0;

    NodeSearchMode  mode = GTE;

    auto last_key = node->key_values.back().first;
    // if lastKey < key?
    if (key_compare_lt(last_key , key))
      // return n+1 th position
      return node->key_values.size();

    // set the binary search range
    unsigned long min = 0, max = node->key_values.size() - 1;

    //used to store comparison result after switch case
    bool compare_result = false;

    while (min < max) {
      // find middle element
      unsigned long mid = (min + max) >> 1;

      // extract the middle key
      auto mid_key = node->key_values[mid].first;

      switch (mode) {
        case GTE:
          compare_result = key_compare_lte(key, mid_key);
          break;
        case NodeSearchMode::GT: break;
      }

      if (compare_result) {
        max = mid;
      } else {
        min = mid + 1;
      }
    }
    return min;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  do_tree_operation(const pid_t node_pid, const KeyType &key,
                    const ValueType& value,
                    const OperationType op_type,
                    const TreeState &state) {

    Node *node = mapping_table_.get_phy_ptr(node_pid);
    return do_tree_operation(node, key, value, op_type, state);
  };

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  do_tree_operation(Node* head, const KeyType &key,
                    const ValueType& value,
                    const OperationType op_type,
                    const TreeState &state) {

    TreeOpResult op_result;

    TreeState child_state;

    // set parent pid
    child_state.parent_pid = head->pid;

    if (op_type != OperationType::search_op &&
        head->get_type() == NodeType::removeNode) {
      // don't permit insert/delete to continue
      // TODO: implement retry looping
      // send failed status
      op_result.status = false;
      return op_result;
    }

    // check if we are already at leaf level
    if (head->is_leaf()){
      if (op_type == OperationType::search_op) {
        op_result = search_leaf_page(head, key, child_state);
      } else {
        // insert/delete operation; try to update
        // delta chain and fetch result
        // no nodes yet, kq is infinite
        child_state.is_kq_inf = true;
        op_result = update_leaf_delta_chain(head, head->pid, key, value,
                                            op_type, child_state);
      }
    } else {

      // flag to indicate if iteration should be continued
      bool continue_itr = true;

      // pointer for iterating
      auto node = head;

      //iterate through the delta chain, based on case
      while (continue_itr && node != nullptr) {
        // Note: explicitly set continue_itr state only if it goes to false

        // switch on the type
        switch (node->get_type()) {
          case indexDelta: {
            // cast the node
            auto delta_node = static_cast<IndexDelta *>(node);

            // Kp < key?
            if (key_compare_lt(delta_node->low, key)){
              // check if kq is infinity or key <= kq
              if(delta_node->is_high_inf ||
                 key_compare_lte(key, delta_node->high)) {
                // recurse into shortcut ptr
                if(delta_node->is_high_inf)
                  child_state.is_kq_inf = true;
                else
                  child_state.kq = delta_node->high;
                op_result = do_tree_operation(delta_node->new_node,
                                              key, value, op_type,
                                              child_state);
                // don't iterate anymore
                continue_itr = false;
              }

            }
            // otherwise descend delta chain

            break;
          }

          case deleteIndex: {
            auto delta_node = static_cast<DeleteIndex *>(node);
            // Kp <= key, key < Kq?
            if (key_compare_lte(delta_node->low, key) &&
                key_compare_lt(key, delta_node->high)) {
              // recurse into shortcut pointer
              op_result = do_tree_operation(delta_node->merge_node, key,
                                            value, op_type, child_state);
              // don't iterate anymore
              continue_itr = false;
            }
            break;
          }

          case deltaSplitInner: {
            auto delta_node = static_cast<DeltaSplitInner *>(node);
            // splitkey <= Key?
            if (key_compare_lte(delta_node->splitKey, key)) {
              // recurse into sibling node, send parent's state
              // update child state in recursive call
              op_result = do_tree_operation(delta_node->new_node, key,
                                            value, op_type, child_state);
              // don't iterate anymore
              continue_itr = false;
            }

            break;
          }

          case mergeInner: {
            auto delta_node = static_cast<MergeInner *>(node);
            // splitkey <= key?
            if (key_compare_lte(delta_node->splitKey, key)) {
              // recurse into node to be deleted
              op_result = do_tree_operation(delta_node->deleting_node, key,
                                            value, op_type, child_state);
              // don't iterate anymore
              continue_itr = false;
            }

            break;
          }

          case inner: {
            auto inner_node = static_cast<InnerNode *>(node);

            if (inner_node->key_values.empty()) {
              //this shouldn't happen
              LOG_ERROR("Failed binary search at page");
              return get_failed_result();
            }

            // find the position of the child to the left of nearest
            // greater key
            auto child_pos = node_key_search(inner_node, key);

            pid_t child_pid = NULL_PID;

            // if kmax is not infinity and search key > kmax
            // go to right sibling inner node
            if (!inner_node->is_kmax_inf &&
                key_compare_lt(inner_node->kmax, key)){
              // check if sibling is null
              if(inner_node->sidelink != NULL_PID){
                // go to sibling
                op_result = do_tree_operation(inner_node->sidelink, key, value,
                                              op_type, state);
              } else {
                // operation failure
                op_result.status = false;
                return op_result;
              }
            } else {
              if(child_pos < inner_node->key_values.size()) {
                // extract child pid from they key value pair
                child_pid = inner_node->key_values[child_pos].second;
                child_state.kq = inner_node->key_values[child_pos].first;
              } else if(child_pos == inner_node->key_values.size()) {
                // otherwise, use the last child pointer
                child_pid = inner_node->last_child;
                child_state.is_kq_inf = true;
              }
              // check the node's level
              if (inner_node->level == 1) {
                // next level is leaf, execute leaf operation
                if (op_type == OperationType::search_op) {
                  // search the leaf page and get result
                  op_result = search_leaf_page(child_pid, key, child_state);
                  // don't iterate anymore
                  continue_itr = false;
                } else {
                  // insert/delete operation; try to update
                  // delta chain and fetch result
                  op_result = update_leaf_delta_chain(child_pid, key, value,
                                                      op_type, child_state);
                  // don't iterate anymore
                  continue_itr = false;
                }
              } else {
                // otherwise recurse into child node
                op_result = do_tree_operation(child_pid, key, value,
                                              op_type, child_state);
                //don't iterate anymore
                continue_itr = false;
              }
            }
            break;
          }

          case leaf:
          case mergeLeaf:
          case removeNode:
          case deltaDelete:
          case deltaInsert:
          case deltaSplitLeaf:
          LOG_ERROR("Invalid cases reached at inner");
            break;
        }

        // move to the next node
        node = node->next;
      }
    }

    bool has_child_split = op_result.has_split;

    // child to go back to unblock consolidate
    pid_t child_pid = op_result.pid;

    op_result.pid = head->pid;

//    if(!op_result.status){
//      // op_result has failed, return
//      return op_result;
//    }

    // the node hasn't split yet
    op_result.has_split = false;

    // if the child has split, install the index delta node
    if(!op_result.is_new_inner_node && has_child_split){
      // index delta node
      Node *node = nullptr;

      // create the index delta
      if(op_result.is_kq_inf){
        // kq is infinity
        node = new IndexDelta(op_result.kp,
                              op_result.split_merge_pid);
      } else {
        // kq is not infinity
        node = new IndexDelta(op_result.kp, op_result.kq,
                              op_result.split_merge_pid);
      }

      do{
        // either do a reqd or forced consolidate
        if (head->chain_length > consolidate_threshold_inner_ ||
            (op_result.has_split && head->has_index_delta)) {

          auto cons_result = consolidate_inner(head);
          if(cons_result.status && cons_result.has_split) {
            // split found at this node, inform parent on return
            op_result.has_split = true;

            // set pid of split page
            op_result.split_merge_pid =
                cons_result.split_child_pid;

            // assume no inner node is created
            op_result.is_new_inner_node =
                cons_result.is_new_inner_node;

            // used to inform parent kp value
            op_result.kp = cons_result.kp;
            op_result.is_kq_inf = false;
            if(state.is_kq_inf)
              op_result.is_kq_inf = true;
            else
              op_result.kq = state.kq;
          }
        }
        // update delta chain
        node->set_next(head);

      }while(!mapping_table_.install_node(head->pid,head, node));

      // index delta has been installed,
      // unblock consolidate in child node
      unblock_consolidate(child_pid);
    }

    // return the result from lower levels.
    // or failure
    return op_result;
  }


  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  search_leaf_page(const pid_t node_pid, const KeyType &key, const TreeState &state) {

    Node *head = mapping_table_.get_phy_ptr(node_pid);
    return search_leaf_page(head, key, state);
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  search_leaf_page(Node *head, const KeyType &key, const TreeState &state) {

    TreeOpResult result;

    // used for unblocking consolidate, if needed
    result.pid = head->pid;

    // TODO: convert to unordered set
    std::set<ValueType, ItemPointerComparator> deleted_val;

    // search always succeeds
    result.status = true;

    // if consolidate is not blocked and threshold is reached
    if (!head->is_consolidate_blocked &&
        head->chain_length > consolidate_threshold_leaf_) {
      // perform consolidation
      auto cons_result = consolidate_leaf(head);
      if (cons_result.status && result.has_split){
        // split has happened, inform parent on return
        result.has_split = true;

        // set pid of split page
        result.split_merge_pid = cons_result.split_child_pid;

        result.is_new_inner_node
            = cons_result.is_new_inner_node;
        // used to inform parent kp value
        result.kp = cons_result.kp;
        result.is_kq_inf = false;
        if(state.is_kq_inf)
          result.is_kq_inf = true;
        else
          result.kq = state.kq;
      }
    }


    bool continue_itr = true;

    // used to iterate the delta chain
    auto node = head;

    while (continue_itr && node != nullptr) {
      switch (node->get_type()) {

        case deltaInsert: {
          auto delta_node = static_cast<DeltaInsert *>(node);
          // check if the key matches
          if (key_compare_eq(delta_node->key, key)) {
            // and if value has not been deleted
            if(deleted_val.find(delta_node->value) == deleted_val.end()) {
              result.values.push_back(delta_node->value);
            }
          }
          break;
        }

        case deltaDelete: {
          auto delta_node = static_cast<DeltaDelete *>(node);

          // check if the key matches the request
          if (key_compare_eq(delta_node->key, key)) {
            // update the set of deleted values
            deleted_val.insert(delta_node->value);
          }
          break;
        }

        case deltaSplitLeaf: {
          auto delta_node = static_cast<DeltaSplitLeaf *>(node);

          // splitKey < key?
          if (key_compare_lt(delta_node->splitKey, key )) {
            // do search on Q and return
            result =  search_leaf_page(delta_node->new_child,
                                       key, state);
            result.pid = head->pid;
            return result;
          }
          break;
        }

        case mergeLeaf: {
          auto delta_node = static_cast<MergeLeaf *>(node);

          // splitKey <= key?
          if (key_compare_lte(delta_node->splitKey, key )) {
            // continue search on R
            result = search_leaf_page(delta_node->deleting_node, key,
                                      state);

            continue_itr = false;
          }
          break;
        }

        case leaf: {
          auto leaf_node = static_cast<LeafNode *>(node);

          // search complete
          if (leaf_node->key_values.empty()) {
            return result;
          }

          // find the position of the child to the left of nearest
          // greater key
          auto child_pos = node_key_search(leaf_node, key);

          if(child_pos >= leaf_node->key_values.size()){
            // key out of range
            break;
          }

          // extract child pid from they key value pair
          auto match = leaf_node->key_values[child_pos];


          // otherwise, key is in this range,
          // check the binary search result
          if (key_compare_eq(match.first, key)) {
            // keys are equal
            // add the non-deleted values to the result
            for (auto it = match.second.begin(); it != match.second.end(); it++) {
              // check if not deleted
              if(deleted_val.find(*it) == deleted_val.end()){
                // value hasn't been deleted, add to the result
                result.values.push_back(*it);
              }
            }
          }

          continue_itr = false;
          break;
        }
        case NodeType::inner: break;
        case NodeType::indexDelta: break;
        case NodeType::deleteIndex: break;
        case NodeType::deltaSplitInner: break;
        case NodeType::mergeInner: break;
        case NodeType::removeNode: break;
      }

      // go to next node
      node = node->next;
    }

    return result;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  update_leaf_delta_chain(const pid_t pid, const KeyType& key,
                          const ValueType& value, const OperationType op_type,
                          const TreeState &state) {
    Node *head = mapping_table_.get_phy_ptr(pid);
    return update_leaf_delta_chain(head, pid, key, value, op_type, state);
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  update_leaf_delta_chain(Node *head, const pid_t pid,  const KeyType& key,
                          const ValueType& value, const OperationType op_type,
                          const TreeState &state) {

    if (head->get_type() == NodeType::removeNode) {
      return get_failed_result();
    }

    auto ptr = head;
    // check if this delta chain contains
    // a split delta
    if(ptr->has_split_delta) {
      // look for the split delta
      while(ptr->get_type() != NodeType::deltaSplitLeaf)
          ptr = ptr->next;

      // delta split reached
      auto delta_split = static_cast<DeltaSplitLeaf *>(ptr);

      // if the key is greater than split key
      // go to the sibling
      if (key_compare_lt(delta_split->splitKey, key)) {
        return update_leaf_delta_chain(delta_split->new_child, key,
                                       value, op_type, state);
      }

      // otherwise, continue from the top of this delta chain (head)
    }

    TreeOpResult result;

    result.pid = pid;

    // if consolidate is not blocked and consolidate
    // threshold has been reached
    if (!head->is_consolidate_blocked &&
        head->chain_length > consolidate_threshold_leaf_) {
      auto cons_result = consolidate_leaf(head);
      // if consolidate succeeded
      if (cons_result.status && cons_result.has_split){
        // split has occured
        result.has_split = true;

        // set pid of split page
        result.split_merge_pid = cons_result.split_child_pid;

        result.is_new_inner_node
            = cons_result.is_new_inner_node;
        // used to inform parent kp value
        result.kp = cons_result.kp;
        result.is_kq_inf = false;
        if(state.is_kq_inf)
          result.is_kq_inf = true;
        else
          result.kq = state.kq;
      }

      result.status = false;
      // update parent nodes and re-try operation
      return result;
    }

    Node *update = nullptr;

    if (op_type == OperationType::insert_op) {
      update = new DeltaInsert(key, value);
    } else {
      // otherwise, it is a delete op
      update = new DeltaDelete(key, value);
    }

    // check if addition is permitted and add
    do {
      // get latest head value
      head = mapping_table_.get_phy_ptr(pid);

      if (head->get_type() == NodeType::removeNode) {
        // free the update
        delete update;

        // operation shouldn't happen here
        return get_failed_result();
      }

      if(head->get_type() == NodeType::deltaSplitLeaf) {
        auto split_delta = static_cast<DeltaSplitLeaf *>(head);

        if (key_compare_lt(split_delta->splitKey, key)){
          // update not happening in this node
          delete update;

          // operation should be performed on the split node, forward state
          // and recurse
          result =  update_leaf_delta_chain(split_delta->new_child,
                                         key, value, op_type, state);
          result.pid = head->pid;
          return result;
        }
      }

      // link to delta chain
      update->set_next(head);
    } while (!mapping_table_.install_node(pid, head, update));

    // operation has completed
    result.status = true;
    return result;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
   search_deleted_kv(const std::vector<std::pair<KeyType, ValueType>> deleted_KV, KVType kv){
    for(auto kv_itr = deleted_KV.begin(); kv_itr != deleted_KV.end(); kv_itr++){
      if(key_val_compare_eq(*kv_itr, kv)){
        // kv has been deleted
        return true;
      }
    }
    // kv has not been deleted
    return false;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  std::vector<ValueType> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  AllKeyScan() {
    // pid of next leaf to visit
    pid_t next_pid = head_leaf_ptr_;

    TreeOpResult result;

    // always succeeds
    result.status = true;

    while(next_pid != NULL_PID){
      // start from the head of the list of leaves
      Node *head = mapping_table_.get_phy_ptr(next_pid);

      std::vector<std::pair<KeyType, ValueType>> deleted_KV;
      while(head){
        switch(head->get_type()){
          case deltaInsert: {
            auto node = static_cast<DeltaInsert *>(head);
            auto kv = std::make_pair(node->key, node->value);

            if(!search_deleted_kv(deleted_KV, kv)){
              // not found, add to result
              result.values.push_back(kv.second);
            }
            break;
          }

          case deltaDelete: {
            auto node = static_cast<DeltaDelete *>(head);
            auto kv = std::make_pair(node->key, node->value);
            if(!search_deleted_kv(deleted_KV, kv)){
              // add kv to deleted_kv
              deleted_KV.push_back(kv);
            }
            break;
          }

          case NodeType::leaf: {
            auto leaf = static_cast<LeafNode *>(head);
            // save the next_pid
            next_pid = leaf->sidelink;

            // iterate through all keys
            for(auto keyval_it = leaf->key_values.begin(); keyval_it != leaf->key_values.end();
                keyval_it++){
              auto key = keyval_it->first;
              // check if no value of this key has been deleted
              auto values = keyval_it->second;

              for(auto val_it = values.begin(); val_it != values.end(); val_it++){
                auto kv = std::make_pair(key, *val_it);
                // check if it is not deleted
                if(!search_deleted_kv(deleted_KV, kv)){
                  // add to result
                  result.values.push_back(kv.second);
                }
              }
            }
            break;
          }
          case NodeType::inner:break;
          case NodeType::indexDelta:break;
          case NodeType::deleteIndex:break;
          case NodeType::deltaSplitInner:break;
          case NodeType::mergeInner:break;
          case NodeType::deltaSplitLeaf:break;
          case NodeType::mergeLeaf:break;
          case NodeType::removeNode:break;
        }

        // descend delta chain
        head = head->next;
      }

      // go to next leaf page
    }

    return result.values;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  unblock_consolidate(const pid_t pid) {
    Node *head = mapping_table_.get_phy_ptr(pid);

    do {
      // atomically try to unblock
      // consolidate at head
      head->is_consolidate_blocked = false;
    }while(!mapping_table_.install_node(pid, head, head));

  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  std::vector<ValueType> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  Search(const KeyType &key) {
    ValueType dummy_val;
    TreeState state;
    TreeOpResult result;
    state.has_index_delta = false;
    state.parent_pid = NULL_PID;
    pid_t root_pid = root_.load(std::memory_order_relaxed);
    do{
      result = do_tree_operation(root_pid, key, dummy_val,
                                 OperationType::search_op, state);
    }while(!result.status);

  #ifdef DEBUG
    print_tree(root_pid);
  #endif
    return result.values;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  Insert(const KeyType &key, const ValueType &value) {
    TreeState state;
    TreeOpResult result;
    state.has_index_delta = false;
    state.parent_pid = NULL_PID;
    pid_t root_pid;
    do{
      root_pid = root_.load(std::memory_order_relaxed);
      result = do_tree_operation(root_pid, key, value,
                                 OperationType::insert_op,
                                 state);
    }while(!result.status);
#ifdef DEBUG
    print_tree(root_pid);
#endif
    memory_usage++;
    return result.status;
  }


  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  Delete(const KeyType &key, const ValueType &val) {
    TreeState state;
    TreeOpResult result;
    state.has_index_delta = false;
    state.parent_pid = NULL_PID;
    pid_t root_pid;
    do{
      root_pid = root_.load(std::memory_order_relaxed);
      result = do_tree_operation(root_pid, key, val,
                                 OperationType::delete_op, state);
    }while(!result.status);
#ifdef DEBUG
    print_tree(root_pid);
#endif
    memory_usage++;
    return result.status;
  }


  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  pid_t BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  getSibling(Node* node){
    while(node->next!= nullptr) {
      node=node->next;
    }
    if(node->is_leaf()){
      TreeNode<KeyType,ValueType>* node_treenode = static_cast<TreeNode<KeyType,ValueType>*>(node);
      return node_treenode->sidelink;}
    else{
      TreeNode<KeyType,pid_t >* node_treenode = static_cast<TreeNode<KeyType,pid_t >*>(node);
      return node_treenode->sidelink;
    }
  }


  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::setSibling(Node* node,pid_t sibling){
    while(node->next!= nullptr)
    {
      node=node->next;
    }
    if(node->is_leaf()) {
      TreeNode<KeyType,ValueType>* node_treenode = static_cast<TreeNode<KeyType,ValueType>*>(node);
      node_treenode->sidelink = sibling;
    }
    else{
      TreeNode<KeyType,pid_t >* node_treenode = static_cast<TreeNode<KeyType,pid_t >*>(node);
      node_treenode->sidelink = sibling;
    }

  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  std::vector<std::pair<KeyType, pid_t>> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  getToBeMovedPairsInner(Node* headNodeP){
  //		vector1.insert( vector1.end(), vector2.begin(), vector2.end() );
    //TODO: Most of the consolidation is similar to this, extend this API to implement consolidate
    Node *copyHeadNodeP = headNodeP;
    while(headNodeP->next!= nullptr){
      headNodeP=headNodeP->next;
    }

    InnerNode* headNodeP1 = static_cast<InnerNode*>(headNodeP);

    std::vector<std::pair<KeyType, pid_t>> wholePairs = headNodeP1->key_values; //TODO: Optimize?

    std::vector<KeyType> deletedPairs;
    pid_t qPID_last_child = headNodeP1->last_child;
    bool pageSplitFlag = false;
    KeyType pageSplitKey;
    auto pageSplitIter = wholePairs.end();

    //handling deltaInsert, deltaDelete, removeNode
    while(copyHeadNodeP->next != nullptr){ //Assuming last node points to nullptr
      switch (copyHeadNodeP->get_type()){
        case indexDelta: {
          //means that there is a split in the child and that we need to insert Kp and with pointer to child
          IndexDelta *curNode = static_cast<IndexDelta*>(copyHeadNodeP);
          int last_flag = 0;
          for (auto it = wholePairs.begin(); it != wholePairs.end(); ++it) {
            //TODO: what if the key is already present in the wholrePairs
            if (!key_compare_lt(curNode->low, it->first)) {
              //not at this position
              continue;
            }
            //curNode->low
            wholePairs.insert(it, std::make_pair((it - 1)->first, curNode->new_node));
            (it - 1)->first = curNode->low;
            last_flag = 1;
            break;
          }
          if (last_flag == 0) {
            wholePairs.insert(wholePairs.end(), std::pair<KeyType, pid_t>(curNode->low, qPID_last_child));
            qPID_last_child = curNode->new_node;
          }
          break;
        }
        case NodeType::deleteIndex:
          //TODO: yet to handle
          break;
        case NodeType::deltaSplitInner: {
          DeltaSplitInner *curNodeSplt = static_cast<DeltaSplitInner *>(copyHeadNodeP);
          if (pageSplitFlag) {
            //TODO: what if the page split key is being deleted?
            if (key_compare_lt(curNodeSplt->splitKey, pageSplitKey)) {
              pageSplitKey = curNodeSplt->splitKey;
            }
          }
          else {
            pageSplitKey = curNodeSplt->splitKey;
            pageSplitFlag = true;
          }
          break;
        }
        case NodeType::mergeInner:
          //TODO: yet to handle
          break;
        case NodeType::removeNode:
          break;
        default:
          break;
      }
      copyHeadNodeP=copyHeadNodeP->next;
    }

    pageSplitIter = std::find_if(wholePairs.begin(), wholePairs.end(),
                                 [&](const std::pair<KeyType,pid_t >& element){
                                   return key_compare_eq(element.first,pageSplitKey);} );
    if(pageSplitIter != wholePairs.end())
    {
      qPID_last_child = pageSplitIter->second;
    }
    //TODO: improve this part later, return cleanly:
    (wholePairs.begin()+std::distance(
        wholePairs.begin(), pageSplitIter)/2)->second=qPID_last_child;

    return std::vector<std::pair<KeyType, pid_t>>(wholePairs.begin()+std::distance(
        wholePairs.begin(), pageSplitIter)/2, pageSplitIter); //TODO: optimize?
    //TODO: check for boundary cases on pageSplitIter
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  std::vector<std::pair<KeyType, std::vector<ValueType>>> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  getToBeMovedPairsLeaf(Node* headNodeP){
    Node *copyHeadNodeP = headNodeP;
    while(headNodeP->next!= nullptr){
      headNodeP=headNodeP->next;
    }

    TreeNode<KeyType,std::vector<ValueType>>* headNodeP1 = static_cast<TreeNode<KeyType,std::vector<ValueType>>*>(headNodeP);
    //TODO:yet to handle for duplicate keys case
    auto wholePairs = headNodeP1->key_values; //TODO: Optimize?
    std::vector<std::pair<KeyType,ValueType>> deletedPairs;

    //handling deltaInsert, deltaDelete, removeNode
    while(copyHeadNodeP->next != nullptr){ //Assuming last node points to nullptr
      switch (copyHeadNodeP->get_type()){
        case deltaInsert:{
          DeltaInsert* deltains = static_cast<DeltaInsert*>(copyHeadNodeP);
          auto it = std::find_if(wholePairs.begin(),wholePairs.end(), [&](const std::pair<KeyType,std::vector<ValueType>>& element){
            return key_compare_eq(element.first,deltains->key);
          });
          if(it != wholePairs.end()){
            (&(it->second))->push_back(deltains->value); //TODO: check correctness
          } else{
            wholePairs.push_back(std::pair<KeyType,std::vector<ValueType>>
                                     (deltains->key,std::vector<ValueType>{deltains->value}));
          }
          break;
        }
        case deltaDelete: {
          DeltaDelete *deltadel = static_cast<DeltaDelete *>(copyHeadNodeP);
          deletedPairs.push_back(std::pair<KeyType,ValueType>(deltadel->key,deltadel->value));
          break;
        }
        default:break;
      }
      copyHeadNodeP=copyHeadNodeP->next;
    }

    for(auto const& elem: deletedPairs){	//TODO: optimize?
      auto it = std::find_if(wholePairs.begin(), wholePairs.end(), [&](const std::pair<KeyType,std::vector<ValueType>>& element) {
        if (key_compare_eq(element.first, elem.first)){
          auto it2 = std::find_if(element.second.begin(), element.second.end(),
                                  [&](const ValueType &element1) { return val_eq(element1, elem.second);});
          return it2!=element.second.end();
        }
        else
          return false;
      });

      if(it != wholePairs.end()){
        wholePairs.erase(it);
      }
      else{
        assert(false);//basically its not supposed to come here
      }
    }

    std::sort(wholePairs.begin(), wholePairs.end(), [&](const std::pair<KeyType, std::vector<ValueType>>& t1,
                                                        const std::pair<KeyType, std::vector<ValueType>>& t2) {
      return key_comparator_(t1.first,t2.first);
    });

    return std::vector<std::pair<KeyType, std::vector<ValueType>>>
        (wholePairs.begin()+std::distance(wholePairs.begin(), wholePairs.end())/2,wholePairs.end()); //TODO: optimize?
  }


  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  checkIfRemoveDelta(Node* head){
    if(head == nullptr)
    {
      return true;
    }

    while(head->next!= nullptr)
    {
      switch(head->get_type())
      {
        case removeNode:
          return true;
        default:
          break;
      }
      head=head->next;
    }
    return false;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::splitPage(pid_t pPID,pid_t rPID,pid_t pParentPID){

    pid_t qPID;Node* splitNode;
    KeyType Kp,Kq;

    Node *headNodeP = mapping_table_.get_phy_ptr(pPID); //get the head node of the deltachain
    Node *headNodeParentP = nullptr;
    Node* sepDel;
    pid_t root_pid = root_.load(std::memory_order_relaxed);

    if(checkIfRemoveDelta(headNodeP))
      return false;

    //if the node to be split is the root
    if(pPID != root_pid) //TODO: do I need to check for the parent too, like nullPID?
    {
      //so basically we need to split the current node into 2 and have a new parent
      //there are 2 cases, if the root node is leaf or not
      headNodeParentP = mapping_table_.get_phy_ptr(pParentPID);
      if(checkIfRemoveDelta(headNodeParentP))
        return false;
    }

    if(headNodeP->is_leaf())
    {
      qPID = static_cast<pid_t>(pid_gen_++);
      LeafNode* newLeafNode = new LeafNode(qPID,rPID); // P->R was there before
      std::vector<std::pair<KeyType, std::vector<ValueType>>> qElemVec = getToBeMovedPairsLeaf(headNodeP);
      newLeafNode->key_values = qElemVec; //TODO: optimize?
      Kp = qElemVec[0].first;
      newLeafNode->record_count = qElemVec.size();
      Kq = qElemVec[newLeafNode->record_count-1].first;
      // TODO: Kq is max among all of P keys before split, if there is something greater than Kq coming
      // TODO: for insert then deltaInsert is created on top of Kp
      mapping_table_.insert_new_pid(qPID, newLeafNode);
      splitNode = new DeltaSplitLeaf(Kp, qPID, headNodeP->record_count-newLeafNode->record_count);
    } else {
      qPID = static_cast<pid_t>(pid_gen_++);
      InnerNode* newInnerNode = new InnerNode(qPID,headNodeP->level,rPID,NULL_PID);
      //TODO: copy vector
      pid_t lastChild;
      std::vector<std::pair<KeyType, pid_t >> qElemVec = getToBeMovedPairsInner(headNodeP);
      Kp = qElemVec[0].first;
      lastChild = qElemVec[0].second;

      qElemVec.erase(qElemVec.begin()); //TODO: Write this cleanly
      newInnerNode->key_values = qElemVec; //TODO: optimize?
      newInnerNode->last_child = lastChild;
      newInnerNode->record_count = qElemVec.size();

      Kq = qElemVec[newInnerNode->record_count-1].first;//TODO: check if right?
      // Kq is max among all of P keys before split, if there is something greater than Kq coming
      // for insert then deltaInsert is created on top of Kp

      mapping_table_.insert_new_pid(qPID, newInnerNode);
      splitNode = new DeltaSplitInner(Kp, qPID, headNodeP->record_count-newInnerNode->record_count-1);
      //TODO: check if -1 is required as Kp is also to be removed
    }

    splitNode->set_next(headNodeP);
    if(!mapping_table_.install_node(pPID, headNodeP, splitNode))
      return false;

    //TODO: Check if retries required?

    //need to handle any specific delta record nodes?1

    //what if P disppears or the parent disappears, or R disappears
    setSibling(headNodeP,NULL_PID); //TODO: check validity

    if(pPID == root_pid) //TODO: Is this case handled: what if root_ is updated during this execution? Do we still use root_pid
    {
      pid_t newRoot = static_cast<pid_t>(pid_gen_++);
      InnerNode* newInnerNode = new InnerNode(newRoot,headNodeP->level+1,NULL_PID, NULL_PID);  //totally new level


      std::vector<std::pair<KeyType, pid_t >> qElemVec = std::vector<std::pair<KeyType, pid_t >>{std::pair<KeyType,pid_t >(Kp,pPID)};

      newInnerNode->key_values = qElemVec; //TODO: optimize?
      newInnerNode->last_child = qPID;
      newInnerNode->record_count = qElemVec.size();

      mapping_table_.insert_new_pid(newRoot, newInnerNode); //TODO: should I try retry here? (And in all other cases)
      //TODO: Check if any inconsistencies can pop in because of non-atomic updates in this scope

      //atomically update the parameter
      return std::atomic_compare_exchange_weak_explicit(
          &root_, &root_pid, newRoot,
          std::memory_order_release, std::memory_order_relaxed); //TODO: Check if retry needed
    }
    else{
      sepDel = new IndexDelta(Kp,Kq,qPID);
      sepDel->set_next(headNodeParentP);
      return mapping_table_.install_node(pParentPID, headNodeParentP, sepDel);
    }
  }


  // step 1 marks start of merge, if fail on step 1 and see a RemoveNodeDelta,
  // abort itself
  // step 2 and step 3 should retry until succeed
  // TODO key range
  // DONE failure case
  // DONE memory management
  // TODO OPTIMIZATION reuse the nodes
  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  merge_page(pid_t pid_l, pid_t pid_r, pid_t pid_parent) {
    memory_usage+=3;
    return (pid_l > 0 && pid_r > 0 && pid_parent > 0);
    Node *ptr_r, *ptr_l, *ptr_parent;
    bool leaf = ptr_r->is_leaf();
    RemoveNode *remove_node_delta = nullptr;
    Node *node_merge_delta = nullptr;
    DeleteIndex *index_term_delete_delta = nullptr;

    // Step 1 marking for delete
    // create remove node delta node
    do {
      ptr_r = mapping_table_.get_phy_ptr(pid_r);
      if (remove_node_delta != nullptr) {
        delete remove_node_delta;
      }
      if (ptr_r->get_type() == NodeType::removeNode) {
        return false;
      }
      remove_node_delta = new RemoveNode();
      remove_node_delta->set_next(ptr_r);
    } while (!mapping_table_.install_node(pid_r, ptr_r, remove_node_delta));

    // Step 2 merging children
    // merge delta node ptr
    // create node merge delta
    do {
      ptr_l = mapping_table_.get_phy_ptr(pid_l);
      if (node_merge_delta != nullptr) {
        delete node_merge_delta;
        node_merge_delta = nullptr;
      }

      int record_count = ptr_l->record_count + ptr_r->record_count;
      if (leaf) {
        // cast the right ptr
        auto leaf_node = static_cast<LeafNode *>(ptr_r);
        // the first right node key
        auto split_key = leaf_node->key_values.front().first;
        // create the delta record
        node_merge_delta = new MergeLeaf(split_key, leaf_node, record_count);
      } else {
        // cast as inner node
        auto inner_node = static_cast<InnerNode *>(ptr_r);
        // the first right node key
        auto split_key = inner_node->key_values.front().first;
        // create delta record
        node_merge_delta = new MergeInner(split_key, inner_node, record_count);
      }
      node_merge_delta->set_next(ptr_l);
      // add to the list
    } while (!mapping_table_.install_node(pid_l, ptr_l, node_merge_delta));


    // Step 3 parent update
    // create index term delete delta
    do {
      ptr_parent = mapping_table_.get_phy_ptr(pid_parent);
      if (index_term_delete_delta != nullptr) {
        delete index_term_delete_delta;
        index_term_delete_delta = nullptr;
      }

      auto left_ptr = static_cast<TreeNode<KeyType, ValueType> *>(ptr_l);
      KeyType left_low_key = left_ptr->key_values.front().first;

      auto right_ptr = static_cast<TreeNode<KeyType, ValueType> *>(ptr_r);
      KeyType right_high_key = right_ptr->key_values.back().first;

      index_term_delete_delta = new DeleteIndex(left_low_key,
                                                right_high_key, pid_l);
      index_term_delete_delta->set_next(ptr_parent);
    } while (!mapping_table_.install_node(pid_parent, ptr_parent,
                                          (Node *) index_term_delete_delta));
    return true;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  ConsolidateResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  consolidate_leaf(Node *node) {
  //          node->chain_length++;

    ConsolidateResult result;
    result.status = false;

    Node *copyHeadNodeP = node;
    Node *secondcopyHeadNodeP = node;
    Node *head_for_gc = node;
  //          bool has_split_delta = false;
  //          KeyType split_key;
  //          pid_t split_child_right;
    while(node->next!= nullptr){
      node=node->next;
      //there can be only one split delta
  //            if(node->get_type() == deltaSplitLeaf){
  //              DeltaSplitLeaf* delSplit = static_cast<DeltaSplitLeaf*>(node);
  //              has_split_delta =  true;
  //              split_key = delSplit->splitKey;
  ////              split_child_right = delSplit->new_child;
  //            }
    }

    if(checkIfRemoveDelta(node)) //TODO: Should I do this here?
    {
      return result;
    }

    LeafNode* headNodeP1 = static_cast<LeafNode*>(node);
    //TODO:yet to handle for duplicate keys case

    auto wholePairs = headNodeP1->key_values; //if split then discarded values wont be there

    std::vector<std::pair<KeyType,ValueType>> deletedPairs;
    std::vector<std::pair<KeyType,ValueType>> insertedPairs;

    //IMPORTANT ASSUMPTION: Any delta corresponding to the the new_child being pointed by the splitdelta, will not go to this node
    //handling deltaInsert, deltaDelete, removeNode
    while(copyHeadNodeP->next != nullptr){ //Assuming last node points to nullptr
      switch (copyHeadNodeP->get_type()){
        case removeNode: {
          //TODO: Check if we have to do something before returning
          return result;
        }
        case deltaInsert:{
          DeltaInsert* deltains = static_cast<DeltaInsert*>(copyHeadNodeP);

  //                if(has_split_delta){
  //                  if(!key_compare_lte(deltains->key,split_key))
  //                    break;
  //                }

          auto it = std::find_if(deletedPairs.begin(),deletedPairs.end(), [&](const std::pair<KeyType,ValueType>& element){
            return key_compare_eq(element.first,deltains->key) && val_eq(element.second, deltains->value);
          });

          if(it == deletedPairs.end())
            insertedPairs.push_back(std::pair<KeyType,ValueType>
                                        (deltains->key,deltains->value));

  //                (&(it->second))->push_back(deltains->value); //TODO: check correctness
          break;
        }
        case deltaDelete: {
          DeltaDelete *deltadel = static_cast<DeltaDelete *>(copyHeadNodeP);
  //                if(has_split_delta){
  //                  if(!key_compare_lte(deltadel->key,split_key))
  //                    break;
  //                }

          auto it = std::find_if(wholePairs.begin(),wholePairs.end(), [&](const std::pair<KeyType,std::vector<ValueType>>& element){
            return key_compare_eq(element.first,deltadel->key);
          });

          if(it != wholePairs.end()){
            while(true) {
              auto itValVec = std::find_if(it->second.begin(), it->second.end(), [&](const ValueType &elementV) {
                return val_eq(elementV, deltadel->value);
              });
              if (itValVec != it->second.end()) {
                it->second.erase(itValVec);
              }
              else{
                //remove the key with empty vector
                if (it->second.empty())
                  wholePairs.erase(it);
                break;
              }
            }
          }
          deletedPairs.push_back(std::pair<KeyType,ValueType>(deltadel->key,deltadel->value));

          break;
        }
        default:break;
      }
      copyHeadNodeP=copyHeadNodeP->next;
    }

    for(auto const& elem: insertedPairs){	//TODO: optimize?
      auto it = std::find_if(wholePairs.begin(), wholePairs.end(), [&](const std::pair<KeyType,std::vector<ValueType>>& element) {
        return key_compare_eq(element.first, elem.first);
      });

      if(it != wholePairs.end()){
        it->second.push_back(elem.second);
      }
      else{
        wholePairs.push_back(std::make_pair(elem.first,std::vector<ValueType>{elem.second}));
      }
    }

    std::sort(wholePairs.begin(), wholePairs.end(), [&](const std::pair<KeyType, std::vector<ValueType>>& t1,
                                                        const std::pair<KeyType, std::vector<ValueType>>& t2) {
      return key_comparator_(t1.first,t2.first);
    });

    //Now get a new node and put in these key_values in it

    LeafNode* newLeafNode = new LeafNode(copyHeadNodeP->pid,
                                         (static_cast<LeafNode*>(copyHeadNodeP))->sidelink); //TODO: Set neighbor pid here

    result.has_split = false;

    // split threshold checking
    if(wholePairs.size()>(unsigned)split_threshold_){
      //call split
      //split should just be sent half the key_values,
      //other half will be stored here itself
      //need to insert splitdelta on top of the current node
      //TODO: need to insert index delta on the parent
      //TODO: Handle root update case
      DeltaSplitLeaf* splitNodeHead = splitPageLeaf(newLeafNode, wholePairs);

      if(!mapping_table_.install_node(copyHeadNodeP->pid, secondcopyHeadNodeP, splitNodeHead)) //Should I cast?
        return result;
      else {
        pid_t root_pid = root_.load(std::memory_order_relaxed);
        if(root_pid == secondcopyHeadNodeP->pid){
          //hence, root is splitting, create new inner node
          pid_t newRoot = static_cast<pid_t>(pid_gen_++);
          InnerNode* newInnerNode = new InnerNode(newRoot,
                                       copyHeadNodeP->level+1,
                                       NULL_PID,
                                       splitNodeHead->new_child);

          std::vector<std::pair<KeyType, pid_t >> qElemVec = std::vector<std::pair<KeyType, pid_t >>{
                  std::pair<KeyType,pid_t >(splitNodeHead->splitKey,copyHeadNodeP->pid)};

          newInnerNode->key_values = qElemVec;
          newInnerNode->record_count = qElemVec.size();

          mapping_table_.insert_new_pid(newRoot, newInnerNode); //TODO: should I try retry here? (And in all other cases)
          //TODO: Check if any inconsistencies can pop in because of non-atomic updates in this scope

          //atomically update the parameter
          if(!std::atomic_compare_exchange_weak_explicit(
                  &root_, &root_pid, newRoot,
                  std::memory_order_release, std::memory_order_relaxed))
          {
            // Clear unused stuff
            return result;
          }
          result.is_new_inner_node = true;
        }
        result.has_split = true;
        result.kp = splitNodeHead->splitKey;
        result.split_child_pid = splitNodeHead->new_child;
        result.status = true;
      }
      //TODO set: result.kp and result.split_child_pid
    } else if(wholePairs.size()<(unsigned)merge_threshold_){
      //call merge
      //TODO
      result.has_merge = true;
    }
    else{
      //TODO: Check correctness of following
      newLeafNode->key_values = wholePairs;
      newLeafNode->record_count = wholePairs.size();

      if(!mapping_table_.install_node(copyHeadNodeP->pid, secondcopyHeadNodeP, newLeafNode))
        return result;
      else
        result.status = true;
  //            TODO: The following :-
  //            if(!status){
  //              dealloc split node (outside gc)
  //              dealloc split delta (outside gc)
  //              deregister from mapping table
  //            }
    }
    add_to_gc_chain(head_for_gc);
    return result;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  DeltaSplitLeaf* BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  splitPageLeaf(LeafNode *node, const std::vector<std::pair<KeyType,std::vector<ValueType>>>& wholePairs){

    node->key_values=std::vector<std::pair<KeyType, std::vector<ValueType>>>
        (wholePairs.begin(),wholePairs.begin()+std::distance(wholePairs.begin(), wholePairs.end())/2);
    node->record_count=node->key_values.size();

    auto qPID = static_cast<pid_t>(pid_gen_++);
    LeafNode* newLeafNode = new LeafNode(qPID,node->sidelink); // P->R was there before
    newLeafNode->key_values = std::vector<std::pair<KeyType, std::vector<ValueType>>>
        (wholePairs.begin()+std::distance(wholePairs.begin(), wholePairs.end())/2,wholePairs.end());
    newLeafNode->record_count = newLeafNode->key_values.size();
    KeyType Kp = node->key_values[node->record_count-1].first;

    mapping_table_.insert_new_pid(qPID, newLeafNode);
    DeltaSplitLeaf* splitNode = new DeltaSplitLeaf(Kp, qPID, node->record_count);
    splitNode->set_next(node);

    node->sidelink=qPID;

    return splitNode;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  ConsolidateResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  consolidate_inner(Node *node) {

    ConsolidateResult result;
    result.status = false;

    Node *copyHeadNodeP = node;
    Node *secondcopyHeadNodeP = node;
    Node *head_for_gc = node;

  //          bool has_split_delta = false;
  //          KeyType split_key;
  //          pid_t split_child_right;

    while(node->next!= nullptr){
      node=node->next;
      //there can be only one split delta
  //            if(node->get_type() == deltaSplitInner){
  //              DeltaSplitInner* delSplit = static_cast<DeltaSplitInner*>(node);
  //              has_split_delta =  true;
  //              split_key = delSplit->splitKey;
  //              split_child_right = delSplit->new_node;
  //            }
    }

    if(checkIfRemoveDelta(node)) //TODO: Should I do this here?
    {
      return result;
    }

    InnerNode* headNodeP1 = static_cast<InnerNode*>(node);
    //TODO: yet to handle for duplicate keys case

    pid_t qPID_last_child = headNodeP1->last_child;

    auto wholePairs = headNodeP1->key_values;

    //IMPORTANT ASSUMPTION: Any delta corresponding to the the new_child being pointed by the splitdelta, will not go to this node
    //handling deltaInsert, deltaDelete, removeNode
    while(copyHeadNodeP->next != nullptr){ //Assuming last node points to nullptr
      switch (copyHeadNodeP->get_type()){
        case removeNode: {
          //TODO: Check if we have to do something before returning
          return result;
        }
        case indexDelta:{
          IndexDelta *curNode = static_cast<IndexDelta*>(copyHeadNodeP);
          int last_flag = 0;
          for (auto it = wholePairs.begin(); it != wholePairs.end(); ++it) {
            //TODO: what if the key is already present in the wholePairs
            if (!key_compare_lt(curNode->low, it->first)) {
              //not at this position
              continue;
            }
            //TODO: Edge cases possible here, check when merge is implemented
            wholePairs.insert(it, std::pair<KeyType, pid_t>(curNode->low, it->second));
            (it+1)->second = curNode->new_node;
            last_flag = 1;
            break;
          }
          if(last_flag == 0) {
            wholePairs.push_back(std::pair<KeyType, pid_t>(curNode->low, qPID_last_child));
            qPID_last_child = curNode->new_node;
          }
          break;
        }
        case deleteIndex:{
          break;
        }
        case deltaSplitInner:{
          break;
        }
        case mergeInner:{
          break;
        }
        default:break;
      }
      copyHeadNodeP=copyHeadNodeP->next;
    }
  //
  //          std::sort(wholePairs.begin(), wholePairs.end(), [&](const std::pair<KeyType, std::vector<ValueType>>& t1,
  //                                                              const std::pair<KeyType, std::vector<ValueType>>& t2) {
  //              return key_comparator_(t1.first,t2.first);
  //          });

    //Now get a new node and put in these key_values in it
    InnerNode* newInnerNode;
    InnerNode* oldInnerNode = static_cast<InnerNode*>(copyHeadNodeP);
    if(oldInnerNode->is_kmax_inf)
      newInnerNode = new InnerNode(copyHeadNodeP->pid,
                                   copyHeadNodeP->level,
                                   oldInnerNode->sidelink,
                                   qPID_last_child);
    else
      newInnerNode = new InnerNode(copyHeadNodeP->pid,
                                   copyHeadNodeP->level,
                                   oldInnerNode->sidelink,
                                   qPID_last_child,
                                   oldInnerNode->kmax);

    result.has_split = false;

    // split threshold checking
    if(wholePairs.size()>(unsigned)split_threshold_){
      //call split
      //split should just be sent half the key_values,
      //other half will be stored here itself
      //need to insert splitdelta on top of the current node
      //TODO: need to insert index delta on the parent
      //TODO: Handle root update case
      DeltaSplitInner* splitNodeHead = splitPageInner(newInnerNode, wholePairs, qPID_last_child);

      if(!mapping_table_.install_node(copyHeadNodeP->pid, secondcopyHeadNodeP, splitNodeHead)) //Should I cast?
        return result;
      else{
        pid_t root_pid = root_.load(std::memory_order_relaxed);
        if(root_pid == secondcopyHeadNodeP->pid){
          //hence, root is splitting, create new inner node
          pid_t newRoot = static_cast<pid_t>(pid_gen_++);
          InnerNode* newInnerNode = new InnerNode(newRoot,
                                                  copyHeadNodeP->level+1,
                                                  NULL_PID,
                                                  splitNodeHead->new_node);

          std::vector<std::pair<KeyType, pid_t >> qElemVec = std::vector<std::pair<KeyType, pid_t >>{
              std::pair<KeyType,pid_t >(splitNodeHead->splitKey,copyHeadNodeP->pid)};

          newInnerNode->key_values = qElemVec;
          newInnerNode->record_count = qElemVec.size();

          mapping_table_.insert_new_pid(newRoot, newInnerNode); //TODO: should I try retry here? (And in all other cases)
          //TODO: Check if any inconsistencies can pop in because of non-atomic updates in this scope

          //atomically update the parameter
          if(!std::atomic_compare_exchange_weak_explicit(
              &root_, &root_pid, newRoot,
              std::memory_order_release, std::memory_order_relaxed))
          {
            // Clear unused stuff
            return result;
          }
          result.is_new_inner_node = true;
        }
        result.has_split = true;
        result.kp = splitNodeHead->splitKey;
        result.split_child_pid = splitNodeHead->new_node;
        result.status = true;
      }

    }
      //merge threshold checking
    else if(wholePairs.size()<(unsigned)merge_threshold_){
      //call merge
      //TODO
      result.has_merge = true;
    }
    else{
      //TODO: Check correctness of following
      newInnerNode->key_values = wholePairs;
      newInnerNode->record_count = wholePairs.size();

      if(!mapping_table_.install_node(copyHeadNodeP->pid, secondcopyHeadNodeP, newInnerNode))
        return result;
      else
        result.status = true;

  //            TODO: The following :-
  //            if(!status){
  //              dealloc split node (outside gc)
  //              dealloc split delta (outside gc)
  //              deregister from mapping table
  //            }
    }
    add_to_gc_chain(head_for_gc);
    return result;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  DeltaSplitInner* BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  splitPageInner(InnerNode *node, const std::vector<std::pair<KeyType, pid_t>>& wholePairs,
                 pid_t qPID_last_child){

    node->key_values=std::vector<std::pair<KeyType, pid_t >>
        (wholePairs.begin(),wholePairs.begin()+std::distance(wholePairs.begin(), wholePairs.end())/2);
    node->record_count=node->key_values.size();

    auto qPID = static_cast<pid_t>(pid_gen_++);
  //          LeafNode* newLeafNode = new LeafNode(qPID,node->sidelink); // P->R was there before
    InnerNode* newInnerNode;
    if(node->is_kmax_inf){
      newInnerNode = new InnerNode(node->pid,
                                   node->level,
                                   node->sidelink,
                                   qPID_last_child);
      //to update the kmax,sidelink, last_child  of left node

    }
    else{
      newInnerNode = new InnerNode(node->pid,
                                   node->level,
                                   node->sidelink,
                                   qPID_last_child,
                                   node->kmax);
      //to update the kmax,sidelink, last_child  of left node
    }
    node->is_kmax_inf=false;
    newInnerNode->key_values = std::vector<std::pair<KeyType, pid_t >>
        (wholePairs.begin()+std::distance(wholePairs.begin(), wholePairs.end())/2,wholePairs.end());
    newInnerNode->record_count = newInnerNode->key_values.size()-1;
    KeyType Kp = newInnerNode->key_values[0].first;
    node->last_child = newInnerNode->key_values[0].second;
    newInnerNode->key_values.erase(newInnerNode->key_values.begin());

    node->kmax = Kp;

    mapping_table_.insert_new_pid(qPID, newInnerNode);

    DeltaSplitInner* splitNode = new DeltaSplitInner(Kp, qPID, node->record_count);
    splitNode->set_next(node);

    node->sidelink=qPID;

    return splitNode;
  }
  // go through pid table, delete chain
  // on merge node, delete right chain
  // TODO things will change when there's GC
    template <typename KeyType, typename ValueType, class KeyComparator,
    class KeyEqualityChecker>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Cleanup() {
      for (pid_t pid = 0; pid < pid_gen_; ++pid) {
#ifdef DEBUG
        std::cout << "Before:\n" << std::endl;
				print_tree(pid);
#endif
        Node * node = mapping_table_.get_phy_ptr(pid);
        if (node == nullptr)
          continue;
        Node * next = node->next;
        while (next != nullptr) {
          delete node;
          node = next;
          next = next->next;
        }
        delete node;
#ifdef DEBUG
        std::cout << "After:\n" << std::endl;
        print_tree(pid);
#endif
      }

      // clear gc chain
      clear_gc_chain();
#ifdef DEBUG
    std::cout << "Final:\n" << std::endl;
    print_tree(root_.load(std::memory_order_relaxed));
#endif
      return true;
    }
    template <typename KeyType, typename ValueType, class KeyComparator,
    class KeyEqualityChecker>
    size_t BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::GetMemoryFootprint() {
      return memory_usage;
    }

  #ifdef DEBUG
  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  print_node(Node *node) {
    std::cout << "----------------" << std::endl;
    std::cout << *node;
  };

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  print_tree(const pid_t& pid) {
    std::cout << "----Printing tree----" << std::endl;
    Node *head = mapping_table_.get_phy_ptr(pid);

    while (head) {
      // traverse tree

      // print current node
      print_node(head);
      switch(head->get_type()) {

        case NodeType::leaf: {
          auto leaf = static_cast<LeafNode *>(head);
          for (unsigned long i=0; i<leaf->key_values.size(); i++){
            std::cout << "KeyIdx:" << i <<
            "\nValues:" << std::endl;
            for(auto &value : leaf->key_values[i].second) {
              std::cout << "(" << ((ItemPointer)value).block
              << "," << ((ItemPointer)value).offset
              << ") ";
            }
            std::cout << std::endl;
          }
          break;
        }
        case NodeType::inner: {
          auto inner = static_cast<InnerNode *>(head);
          for(unsigned long i=0; i < inner->key_values.size(); i++){
            std::cout << "KeyIdx:" << i << std::endl;
            print_tree(inner->key_values[i].second);
          }
          std::cout << "RightMostChild:" << std::endl;
          print_tree(inner->last_child);
          break;
        }

        case NodeType::indexDelta: {
          auto delta = static_cast<IndexDelta *>(head);
          std::cout << "New Index Delta Node:" << delta->new_node <<
          std::endl;
          print_tree(delta->new_node);
          break;
        }
        case NodeType::deleteIndex: {
          auto delta = static_cast<DeleteIndex *>(head);
          std::cout << "Shortcut to merge Node:" << delta->merge_node <<
          std::endl;
          print_tree(delta->merge_node);
          break;
        }
        case NodeType::deltaSplitInner: {
          auto delta = static_cast<DeltaSplitInner *>(head);
          std::cout << "Shortcut to new node:" << delta->new_node <<
          std::endl;
          print_tree(delta->new_node);
          break;
        }
        case NodeType::mergeInner: {
          auto delta = static_cast<MergeInner *>(head);
          std::cout << "Shortcut to deleting node:" << delta->deleting_node <<
          std::endl;
          print_tree(delta->deleting_node->pid);
          break;
        }
        case NodeType::deltaInsert: {
          auto delta = static_cast<DeltaInsert *>(head);
          std::cout << "Delta Inserted value:" << "("
          << ((ItemPointer)delta->value).block
          << "," << ((ItemPointer)delta->value).offset
          << ")" << std::endl;
          break;
        }
        case NodeType::deltaDelete: {
          auto delta = static_cast<DeltaDelete *>(head);
          std::cout << "Delta deleted value:" << "(" <<
          ((ItemPointer)delta->value).block
          << "," << ((ItemPointer)delta->value).offset
          << ")" << std::endl;
          break;
        }
        case NodeType::deltaSplitLeaf: {
          auto delta = static_cast<DeltaSplitLeaf *>(head);
          std::cout << "Pointer to new child:" << delta->new_child <<
          std::endl;
          print_tree(delta->new_child);
          break;
        }
        case NodeType::mergeLeaf: {
          auto delta = static_cast<MergeLeaf *>(head);
          std::cout << "Pointer to deleting node:" <<
          delta->deleting_node <<
          std::endl;
          print_tree(delta->deleting_node->pid);
          break;
        }
        case NodeType::removeNode: {
          break;
        }
      }
      std::cout << "----------------" << std::endl;
      // descend the delta chain
      head = head->next;
    }

    std::cout << "----Finished Printing tree----" << std::endl;

  };
  #endif

  // Explicit template instantiations

  template class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1>,
      IntsEqualityChecker<1>>;
  template class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2>,
      IntsEqualityChecker<2>>;
  template class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3>,
      IntsEqualityChecker<3>>;
  template class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4>,
      IntsEqualityChecker<4>>;
  template class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4>,
      GenericEqualityChecker<4>>;
  template class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8>,
      GenericEqualityChecker<8>>;
  template class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12>,
      GenericEqualityChecker<12>>;
  template class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16>,
      GenericEqualityChecker<16>>;
  template class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24>,
      GenericEqualityChecker<24>>;
  template class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32>,
      GenericEqualityChecker<32>>;
  template class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48>,
      GenericEqualityChecker<48>>;
  template class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64>,
      GenericEqualityChecker<64>>;
  template class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96>,
      GenericEqualityChecker<96>>;
  template class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128>,
      GenericEqualityChecker<128>>;
  template class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256>,
      GenericEqualityChecker<256>>;
  template class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512>,
      GenericEqualityChecker<512>>;

  template class BWTree<TupleKey, ItemPointer, TupleKeyComparator,
      TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
