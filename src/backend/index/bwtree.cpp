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

#define TODO

namespace peloton {
namespace index {

typedef unsigned int pid_t;

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

      if(result.needs_split) {
        // find the position of the child to the left of nearest
        // greater key
        auto child_pos = node_key_search(inner_node, key);

        // extract child pid from they key value pair
        pid_t child_pid = inner_node->key_values[child_pos].second;

        // store sibling's pid as null pid initially
        pid_t sibling_pid = NULL_PID;

        // check if we came from the right place
        if( result.split_merge_pid == child_pid ) {
          // check if we can get a sibling
          auto keyval_size = inner_node->key_values.size();
          if(child_pos < keyval_size - 1) {
            sibling_pid = inner_node->key_values[child_pos+1].second;
          } else if(child_pid == keyval_size) {
            sibling_pid = inner_node->last_child;
          }
        }
         // invoke split page
        splitPage(child_pid, sibling_pid, inner_node->pid);
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

  // TODO: convert to unordered set
  std::vector<ValueType> deleted_val;

  if (head->get_type() == NodeType::removeNode) {
    // we have a remove node delta, end search and try again
    return get_failed_result();
  }

  if (head->chain_length > consolidate_threshold_leaf_) {
    // perform consolidation
    consolidate(head);
  }

  bool continue_itr = true;

  TreeOpResult result;

  // used to iterate the delta chain
  auto node = head;

  while (continue_itr && node != nullptr) {

    switch (node->get_type()) {

    case deltaInsert: {
      auto delta_node = static_cast<DeltaInsert *>(node);
      if (key_compare_eq(delta_node->key, key)) {
        bool found_flag = false;
        // check if the key matches and the key,value pair has not been deleted
        for(auto it = deleted_val.begin(); it != deleted_val.end(); it++){
          if(val_eq(*it, delta_node->value)){
            // the value has been deleted, break
            found_flag = true;
          }
        }
        if (!found_flag)
          // the value has not been deleted, add result
          result.values.push_back(delta_node->value);
      }
      break;
    }

    case deltaDelete: {
      auto delta_node = static_cast<DeltaDelete *>(node);

      // check if the key matches the request
      if (key_compare_eq(delta_node->key, key)) {
        // update the set of delelted values
        deleted_val.push_back(delta_node->value);
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

      // search complete
      if (leaf_node->key_values.empty()) {
        return result;
      }

      // find the position of the child to the left of nearest
      // greater key
      auto child_pos = node_key_search(leaf_node, key);

      // extract child pid from they key value pair
      auto match = leaf_node->key_values[child_pos];

      // check the binary search result
      if (key_compare_eq(match.first, key)) {
        // keys are equal
        // add the non-deleted values to the result
        for (auto it = match.second.begin(); it != match.second.end(); it++) {
          // check if not deleted
          for (auto del_it = deleted_val.begin(); del_it != deleted_val.end(); del_it++){
            if(val_eq(*del_it, *it)){
              // value has been deleted, break
              break;
            }
          }
          // value hasn't been deleted, add to the result
          result.values.push_back(*it);
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

  result.needs_split = false;
  if (head->record_count > split_threshold_) {
    result.needs_split = true;
  }

  result.needs_merge = false;
	// don't merge if root is a leaf node, despite merge threshold
  pid_t root_pid = root_.load(std::memory_order_relaxed);

  if (head->record_count < merge_threshold_ && !head->pid == root_pid) {
    result.needs_merge = true;
  }

  result.status = true;

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
      update = new DeltaDelete(key, value);
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
      // TODO: assuming sibling pid isn't required
      result.split_merge_pid = head->pid;
    }

    result.needs_merge = false;

    // don't merge if we are at root
    if (head->record_count < merge_threshold_ && !head->pid == root_) {
      result.needs_merge = true;
      // TODO: assuming sibling pid isn't required
      result.split_merge_pid = head->pid;
    }

    return result;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  std::vector<ValueType> BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  Search(const KeyType &key) {
    ValueType dummy_val;
    pid_t root_pid = root_.load(std::memory_order_relaxed);
    auto result = do_tree_operation(root_pid, key, dummy_val, OperationType::search_op);
#ifdef DEBUG
		print_tree(root_pid);
#endif
    return result.values;
  }

  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  Insert(const KeyType &key, const ValueType &value) {
    pid_t root_pid = root_.load(std::memory_order_relaxed);
    TreeOpResult result = do_tree_operation(root_pid, key, value, OperationType::insert_op);
    return result.status;
  }


  template <typename KeyType, typename ValueType, class KeyComparator,
      class KeyEqualityChecker>
  bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
  Delete(const KeyType &key, const ValueType &val) {
    pid_t root_pid = root_.load(std::memory_order_relaxed);
    TreeOpResult result = do_tree_operation(root_pid, key, val, OperationType::delete_op);

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
      LeafNode* newLeafNode = new LeafNode(pPID,rPID); // P->R was there before
      qPID = static_cast<pid_t>(pid_gen_++);
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
      InnerNode* newInnerNode = new InnerNode(pPID,headNodeP->level,rPID,NULL_PID);
      qPID = static_cast<pid_t>(pid_gen_++);
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
      InnerNode* newInnerNode = new InnerNode(pPID,headNodeP->level+1,NULL_PID, NULL_PID);  //totally new level
      pid_t newRoot = static_cast<pid_t>(pid_gen_++);

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
//          node->get_type() == NodeType::mergeLeaf) {
//      	MergeInner *mnode = static_cast<MergeInner *>(node);
//        delete_queue.push_back((Node*) mnode->deleting_node);
//      }
//      delete node;
//      node = next;
//      next = next->next;
//    }
//    delete node;
//  }
//  for (auto itr = delete_queue.begin(); itr != delete_queue.end(); itr++) {
//  	Node * node = *itr;
//    Node * next = node->next;
//    while (next != nullptr) {
//      delete node;
//      node = next;
//      next = next->next;
//    }
//    delete node;
//  }
//  pid_t root_pid = root_.load(std::memory_order_relaxed);
//  Node *node = mapping_table_.get_phy_ptr(root_pid);
//  delete node;
  return true;
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

  while (head->next) {
    // traverse tree

    // print current node
    print_node(head);
    switch(head->get_type()) {

      case NodeType::leaf: {
        auto leaf = static_cast<LeafNode *>(head);
        for (unsigned long i=0; i<leaf->key_values.size(); i++){
          std::cout << "KeyCount:" << i <<
          "\nValues:" << std::endl;
          for(auto &value : leaf->key_values[i].second) {
            std::cout << "(" << ((ItemPointer)value).block
            << "," << ((ItemPointer)value).offset
            << ")\t";
          }
          std::cout << std::endl;
        }
        break;
      }
      case NodeType::inner: {
        auto inner = static_cast<InnerNode *>(head);
        for(unsigned long i=0; i < inner->key_values.size(); i++){
          std::cout << "KeyCount:" << i;
          print_tree(inner->key_values[i].second);
        }
        break;
      }

      case NodeType::indexDelta: {
        auto delta = static_cast<IndexDelta *>(head);
        std::cout << "New Index Delta Node:" << delta->new_node <<
        std::endl;
        break;
      }
      case NodeType::deleteIndex: {
        auto delta = static_cast<DeleteIndex *>(head);
        std::cout << "Shortcut to merge Node:" << delta->merge_node <<
        std::endl;
        break;
      }
      case NodeType::deltaSplitInner: {
        auto delta = static_cast<DeltaSplitInner *>(head);
        std::cout << "Shortcut to new node:" << delta->new_node <<
        std::endl;
        break;
      }
      case NodeType::mergeInner: {
        auto delta = static_cast<MergeInner *>(head);
        std::cout << "Shortcut to deleting node:" << delta->deleting_node <<
        std::endl;
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
        break;
      }
      case NodeType::mergeLeaf: {
        auto delta = static_cast<MergeLeaf *>(head);
        std::cout << "Pointer to deleting node:" <<
        delta->deleting_node <<
        std::endl;
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
