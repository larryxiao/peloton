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

#include "backend/index/bwtree.h"
#define TODO

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker> template <typename K, typename V>
unsigned long BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
node_key_search(const TreeNode<K, V> *node, const KeyType &key) {

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
                  const OperationType op_type) {

  Node *node = mapping_table_.get_phy_ptr(node_pid);
  return do_tree_operation(node, key, value, op_type);
};

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
do_tree_operation(Node* head, const KeyType &key,
                  const ValueType& value,
                  const OperationType op_type) {

  if (head->get_type() == NodeType::removeNode) {
    // we have a remove node delta, end search and try again
    return get_failed_result();
  }

  if (head->chain_length > consolidate_threshold_inner_) {
    consolidate(head);
  }

	// store result from recursion, if any
	TreeOpResult result = get_failed_result();

	// check if we are already at leaf level
	if (head->is_leaf()){
		if (op_type == OperationType::search_op) {
			result = search_leaf_page(head, key);
		} else {
			// insert/delete operation; try to update
			// delta chain and fetch result
			result = update_leaf_delta_chain(head, head->pid, key, value, op_type);
		}

		return result;
	}

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
      // Kp <= key, key < Kq?
      if (key_compare_lte(delta_node->low, key) &&
          key_compare_lt(key, delta_node->high)) {
        // recurse into shortcut pointer
        result = do_tree_operation(delta_node->new_node,
                                   key, value, op_type);
        // don't iterate anymore
        continue_itr = false;
      }
      break;
    }

    case deleteIndex: {
      auto delta_node = static_cast<DeleteIndex *>(node);
      // Kp <= key, key < Kq?
      if (key_compare_lte(delta_node->low, key) &&
          key_compare_lt(key, delta_node->high)) {
        // recurse into shortcut pointer
        result = do_tree_operation(delta_node->merge_node, key,
                                   value, op_type);
        // don't iterate anymore
        continue_itr = false;
      }
      break;
    }

    case deltaSplitInner: {
      auto delta_node = static_cast<DeltaSplitInner *>(node);
      // splitkey <= Key?
      if (key_compare_lte(delta_node->splitKey, key)) {
        // recurse into new node
        result = do_tree_operation(delta_node->new_node, key, value,
                                   op_type);
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
        result = do_tree_operation(delta_node->deleting_node, key,
                                   value, op_type);
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

      // extract child pid from they key value pair
      pid_t child_pid = inner_node->key_values[child_pos].second;
      // check the node's level

      if (inner_node->level == 1) {
        // next level is leaf, execute leaf operation
        if (op_type == OperationType::search_op) {
          // search the leaf page and get result
          result = search_leaf_page(child_pid, key);
          // don't iterate anymore
          continue_itr = false;
        } else {
          // insert/delete operation; try to update
          // delta chain and fetch result
          result = update_leaf_delta_chain(child_pid, key, value, op_type);
          // don't iterate anymore
          continue_itr = false;
        }
      } else {
        // otherwise recurse into child node
        result = do_tree_operation(child_pid, key, value, op_type);
        //don't iterate anymore
        continue_itr = false;
      }

      // check if child node has requested for merge
      if (result.needs_merge) {
        // TODO: Handle leftmost edge case, see WIKI

        // where we came from
        pid_t right = child_pid;

        // what we are merging with
        pid_t left = inner_node->key_values[child_pos - 1].second;

        // Merge
        merge_page(left, right, inner_node->pid);
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


  result.needs_split = false;

  if (head->record_count > split_threshold_) {
    // TODO:split this node
    result.needs_split = true;
  }

  result.needs_merge = false;

  if (head-> record_count < merge_threshold_) {
    // merge this node
    result.needs_merge = true;
  }

  // return the result from lower levels.
  // or failure
  return result;
}


template <typename KeyType, typename ValueType, class KeyComparator,
		class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
search_leaf_page(const pid_t node_pid, const KeyType &key) {

	Node *head = mapping_table_.get_phy_ptr(node_pid);
	return search_leaf_page(head, key);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
search_leaf_page(Node *head, const KeyType &key) {

  if (head->get_type() == NodeType::removeNode) {
    // we have a remove node delta, end search and try again
    return get_failed_result();
  }

  if (head->chain_length > consolidate_threshold_leaf_) {
    // perform consolidation
    consolidate(head);
  }

  bool continue_itr = true;

  TreeOpResult result = get_failed_result();

  // used to iterate the delta chain
  auto node = head;

  while (continue_itr && node != nullptr) {

    switch (node->get_type()) {

    case deltaInsert: {
      auto delta_node = static_cast<DeltaInsert *>(node);

      if (key_compare_eq(delta_node->key, key)) {
        // node has been inserted, prepare result
        result = make_search_result(delta_node->value);

        continue_itr = false;
      }

      break;
    }

    case deltaDelete: {
      auto delta_node = static_cast<DeltaDelete *>(node);

      if (key_compare_eq(delta_node->key, key)) {
        // node has been deleted, fail the search
        result = make_search_fail_result();

        continue_itr = false;
      }
      break;
    }

    case deltaSplitLeaf: {
      auto delta_node = static_cast<DeltaSplitLeaf *>(node);

      // splitKey <= key?
      if (key_compare_lte(delta_node->splitKey, key )) {
        // continue search on Q
        result = search_leaf_page(delta_node->new_child, key);

        continue_itr = false;
      }
      break;
    }

    case mergeLeaf: {
      auto delta_node = static_cast<MergeLeaf *>(node);

      // splitKey <= key?
      if (key_compare_lte(delta_node->splitKey, key )) {
        // continue search on R
        result = search_leaf_page(delta_node->deleting_node, key);

        continue_itr = false;
      }
      break;
    }

    case leaf: {
      auto leaf_node = static_cast<LeafNode *>(node);

      if (leaf_node->key_values.empty()) {
        //this shouldn't happen
        LOG_ERROR("Failed binary search at page");
        return get_failed_result();
      }

      // find the position of the child to the left of nearest
      // greater key
      auto child_pos = node_key_search(leaf_node, key);

      // extract child pid from they key value pair
      auto match = leaf_node->key_values[child_pos];

      // check the binary search result
      if (key_compare_eq(match.first, key)) {
        // keys are equal
        result =  make_search_result(match.second);
      } else {
        result = make_search_fail_result();
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

  result.needs_split = false;
  if (head->record_count > split_threshold_) {
    result.needs_split = true;
  }

  result.needs_merge = false;
	// don't merge if root is a leaf node, despite merge threshold
  if (head->record_count < merge_threshold_ && !head->pid == root_) {
    result.needs_merge = true;
  }

  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
		class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
update_leaf_delta_chain(const pid_t pid, const KeyType& key,
												const ValueType& value, const OperationType op_type) {

	Node *head = mapping_table_.get_phy_ptr(pid);
	return update_leaf_delta_chain(head, pid, key, value, op_type);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
update_leaf_delta_chain(Node *head, const pid_t pid,  const KeyType& key,
                        const ValueType& value, const OperationType op_type) {

  if (head->get_type() == NodeType::removeNode) {
    return get_failed_result();
  }

  if (head->chain_length > consolidate_threshold_leaf_) {
    consolidate(head);
  }

  Node *update = nullptr;

  if (op_type == OperationType::insert_op) {
    update = new DeltaInsert(key, value);
  } else {
    // otherwise, it is a delete op
    update = new DeltaDelete(key);
  }

  // try till update succeeds
  do {
    // get latest head value
    head = mapping_table_.get_phy_ptr(pid);

    if (head->get_type() == NodeType::removeNode) {
      return get_failed_result();
    }

    // link to delta chain
    update->set_next(head);
  } while (!mapping_table_.install_node(pid, head, update));

  // set head as the new node we installed
  head = update;

  TreeOpResult result = get_update_success_result();

  result.needs_split = false;
  if (head->record_count > split_threshold_) {
    result.needs_split = true;
  }

  result.needs_merge = false;

	// don't merge if we are at root
  if (head->record_count < merge_threshold_ && !head->pid == root_) {
    result.needs_merge = true;
  }

  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
Search(const KeyType &key, ValueType *value) {

  ValueType dummy_val;
  auto result = do_tree_operation(root_, key, dummy_val, OperationType::search_op);

  if (result.status && result.is_valid_value) {
    // search has succeeded
    *value = result.value;
    return true;
  }

  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
Insert(const KeyType &key, const ValueType &value) {

  TreeOpResult result = do_tree_operation(root_, key, value, OperationType::insert_op);
  return result.status;
}


template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
Delete(const KeyType &key) {
  ValueType dummy_val;

  TreeOpResult result = do_tree_operation(root_, key, dummy_val, OperationType::delete_op);

  return result.status;
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
    return (pid_l > 0 && pid_r > 0 && pid_parent > 0);
//  Node *ptr_r, *ptr_l, *ptr_parent;
//  bool leaf = ptr_r->is_leaf();
//  RemoveNode *remove_node_delta = nullptr;
//  Node *node_merge_delta = nullptr;
//  DeleteIndex *index_term_delete_delta = nullptr;
//
//  // Step 1 marking for delete
//  // create remove node delta node
//  do {
//    ptr_r = mapping_table_.get_phy_ptr(pid_r);
//    if (remove_node_delta != nullptr) {
//      delete remove_node_delta;
//    }
//    if (ptr_r->get_type() == NodeType::removeNode) {
//      return false;
//    }
//    remove_node_delta = new RemoveNode();
//    remove_node_delta->set_next(ptr_r);
//  } while (!mapping_table_.install_node(pid_r, ptr_r, remove_node_delta));
//
//  // Step 2 merging children
//  // merge delta node ptr
//  // create node merge delta
//  do {
//    ptr_l = mapping_table_.get_phy_ptr(pid_l);
//    if (node_merge_delta != nullptr) {
//      delete node_merge_delta;
//      node_merge_delta = nullptr;
//    }
//
//    int record_count = ptr_l->record_count + ptr_r->record_count;
//    if (leaf) {
//      // cast the right ptr
//      auto leaf_node = static_cast<LeafNode *>(ptr_r);
//      // the first right node key
//      auto split_key = leaf_node->key_values.front().first;
//      // create the delta record
//      node_merge_delta = new MergeLeaf(split_key, leaf_node, record_count);
//    } else {
//      // cast as inner node
//      auto inner_node = static_cast<InnerNode *>(ptr_r);
//      // the first right node key
//      auto split_key = inner_node->key_values.front().first;
//      // create delta record
//      node_merge_delta = new MergeInner(split_key, inner_node, record_count);
//    }
//    node_merge_delta->set_next(ptr_l);
//    // add to the list
//  } while (!mapping_table_.install_node(pid_l, ptr_l, node_merge_delta));
//
//
//  // Step 3 parent update
//  // create index term delete delta
//  do {
//    ptr_parent = mapping_table_.get_phy_ptr(pid_parent);
//    if (index_term_delete_delta != nullptr) {
//      delete index_term_delete_delta;
//      index_term_delete_delta = nullptr;
//    }
//
//    auto left_ptr = static_cast<TreeNode<KeyType, ValueType> *>(ptr_l);
//    KeyType left_low_key = left_ptr->key_values.front().first;
//
//    auto right_ptr = static_cast<TreeNode<KeyType, ValueType> *>(ptr_r);
//    KeyType right_high_key = right_ptr->key_values.back().first;
//
//    index_term_delete_delta = new DeleteIndex(left_low_key,
//        right_high_key, pid_l);
//    index_term_delete_delta->set_next(ptr_parent);
//  } while (!mapping_table_.install_node(pid_parent, ptr_parent,
//                                        (Node *) index_term_delete_delta));
//  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
consolidate(Node *node) {
  node->chain_length = 1;

}
// go through pid table, delete chain
// on merge node, delete right chain
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Cleanup() {
//  std::vector<Node *> delete_queue;
//  for (pid_t pid = 0; pid < pid_gen_; ++pid) {
//    Node * node = mapping_table_.get_phy_ptr(pid);
//    if (node == nullptr)
//      continue;
//    Node * next = node->next;
//    while (next != nullptr) {
//      if (node->get_type() == NodeType::mergeInner ||
//          node->get_type() == NodeType::mergeLeaf)
//        delete_queue.insert(node->deleting_node);
//      delete node;
//      node = next;
//      next = next->next;
//    }
//    delete node;
//  }
//  for (auto node = delete_queue.begin(); node != delete_queue.end(); node++) {
//    Node * next = node->next;
//    while (next != nullptr) {
//      delete node;
//      node = next;
//      next = next.next;
//    }
//    delete node;
//  }
  Node *node = mapping_table_.get_phy_ptr(root_);
  delete node;
  return true;
}

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
