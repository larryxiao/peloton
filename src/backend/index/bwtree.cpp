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
#include "bwtree_index.h"

#define TODO

namespace peloton {
namespace index {

typedef unsigned int pid_t;

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker> template <typename K, typename V>
unsigned long BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
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
    class ValueComparator, class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
do_tree_operation(const pid_t node_pid, const KeyType &key,
                  const ValueType& value,
                  const OperationType op_type) {

  Node *node = mapping_table_.get_phy_ptr(node_pid);
  return do_tree_operation(node, key, value, op_type);
};

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
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
      pid_t child_pid = inner_node->key_values[child_pos].second[0];
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
        pid_t left = inner_node->key_values[child_pos - 1].second[0];

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
    class ValueComparator, class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
search_leaf_page(const pid_t node_pid, const KeyType &key) {

	Node *head = mapping_table_.get_phy_ptr(node_pid);
	return search_leaf_page(head, key);
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
search_leaf_page(Node *head, const KeyType &key) {

  // set of deleted values for this key
  std::unordered_set<ValueType, std::hash<ValueType>, ValueComparator> deleted_val;

  if (head->get_type() == NodeType::removeNode) {
    // we have a remove node delta, end search and try again
    return get_failed_result();
  }

  if (head->chain_length > consolidate_threshold_leaf_) {
    // perform consolidation
    consolidate(head);
  }

  bool continue_itr = true;

  TreeOpResult result = TreeOpResult(true);

  // used to iterate the delta chain
  auto node = head;

  while (continue_itr && node != nullptr) {

    switch (node->get_type()) {

    case deltaInsert: {
      auto delta_node = static_cast<DeltaInsert *>(node);

      // check if the key matches and the key,value pair has not been deleted
      if (key_compare_eq(delta_node->key, key) &&
            (deleted_val.find(delta_node->val) == deleted_val.end())) {
        // node has not been deleted, add result
        result.values.push_back(delta_node->val);
      }
      break;
    }

    case deltaDelete: {
      auto delta_node = static_cast<DeltaDelete *>(node);

      // check if the key matches the request
      if (key_compare_eq(delta_node->key, key)) {
        // update the set of delelted values
        deleted_val.insert(delta_node->val);
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
        for(auto it = match.second.begin(); it != match.second.end(); it++) {
          // check if not deleted
          if (deleted_val.find(*it) == deleted_val.end()) {
            // add value to the result
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
    class ValueComparator, class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
update_leaf_delta_chain(const pid_t pid, const KeyType& key,
												const ValueType& value, const OperationType op_type) {

	Node *head = mapping_table_.get_phy_ptr(pid);
	return update_leaf_delta_chain(head, pid, key, value, op_type);
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
typename BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
TreeOpResult BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
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
  }

  result.needs_merge = false;

	// don't merge if we are at root
  if (head->record_count < merge_threshold_ && !head->pid == root_) {
    result.needs_merge = true;
  }

  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
std::vector<ValueType> BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
Search(const KeyType &key) {
  ValueType dummy_val;
  auto result = do_tree_operation(root_, key, dummy_val, OperationType::search_op);
  return result.values;
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
Insert(const KeyType &key, const ValueType &value) {

  TreeOpResult result = do_tree_operation(root_, key, value, OperationType::insert_op);
  return result.status;
}


template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
Delete(const KeyType &key) {
  ValueType dummy_val;

  TreeOpResult result = do_tree_operation(root_, key, dummy_val, OperationType::delete_op);

  return result.status;
}


template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
pid_t BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
getSibling(Node* node){
  while(node->next!= nullptr)
  {
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
    class ValueComparator, class KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::setSibling(Node* node,pid_t sibling){
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
    class ValueComparator, class KeyEqualityChecker>
std::vector<std::pair<KeyType, std::vector<pid_t>>> BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
getToBeMovedPairsInner(Node* headNodeP){
//		vector1.insert( vector1.end(), vector2.begin(), vector2.end() );
  //TODO: Most of the consolidation is similar to this, extend this API to implement consolidate
  Node *copyHeadNodeP = headNodeP;
  while(headNodeP->next!= nullptr){
    headNodeP=headNodeP->next;
  }

  InnerNode* headNodeP1 = static_cast<InnerNode*>(headNodeP);
  //TODO:yet to handle for duplicate keys case
  std::vector<std::pair<KeyType, std::vector<pid_t> >> wholePairs = headNodeP1->key_values; //TODO: Optimize?
//		std::vector<KeyType> insertedKeys;
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
        IndexDelta *curNode = static_cast<IndexDelta *>(copyHeadNodeP);
        int last_flag = 0;
        for (auto it = wholePairs.begin(); it != wholePairs.end(); ++it) {
          //TODO: what if the key is already present in the wholrePairs
          if (!key_compare_lt(curNode->low, it->first)) {
            //not at this position
            continue;
          }
          //curNode->low
          wholePairs.insert(it, std::pair<KeyType, pid_t>((it - 1)->first, curNode->new_node));
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
    qPID_last_child = pageSplitIter->second.front();
  }
  //TODO: improve this part later, return cleanly:
  (wholePairs.begin()+std::distance(
          wholePairs.begin(), pageSplitIter)/2)->second.front()=qPID_last_child;

  return std::vector<std::pair<KeyType, std::vector<pid_t>>>(wholePairs.begin()+std::distance(
          wholePairs.begin(), pageSplitIter)/2, pageSplitIter); //TODO: optimize?
  //TODO: check for boundary cases on pageSplitIter
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
std::vector<std::pair<KeyType, ValueType>>
BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
getToBeMovedPairsLeaf(Node* headNodeP){
  Node *copyHeadNodeP = headNodeP;
  while(headNodeP->next!= nullptr){
    headNodeP=headNodeP->next;
  }

  TreeNode<KeyType,ValueType>* headNodeP1 = static_cast<TreeNode<KeyType,ValueType>*>(headNodeP);
  //TODO:yet to handle for duplicate keys case
  std::vector<std::pair<KeyType, ValueType>> wholePairs = headNodeP1->key_values; //TODO: Optimize?
  std::vector<KeyType> deletedPairs;

  //handling deltaInsert, deltaDelete, removeNode
  while(copyHeadNodeP->next != nullptr){ //Assuming last node points to nullptr
    switch (copyHeadNodeP->get_type()){
      case deltaInsert:{
        DeltaInsert* deltains = static_cast<DeltaInsert*>(copyHeadNodeP);
        wholePairs.push_back(std::pair<KeyType,ValueType>(deltains->key,deltains->val));
        break;
      }
      case deltaDelete: {
        DeltaDelete *deltadel = static_cast<DeltaDelete *>(copyHeadNodeP);
        deletedPairs.push_back(deltadel->key);
        break;
      }
      default:break;
    }
    copyHeadNodeP=copyHeadNodeP->next;
  }

  for(auto const& elem: deletedPairs){	//TODO: optimize?
    auto it = std::find_if(wholePairs.begin(), wholePairs.end(), [&](const std::pair<KeyType,ValueType>& element){ return key_compare_eq(element.first,elem);} );

    if(it != wholePairs.end()){
      wholePairs.erase(it);
    }
  }
  std::sort(wholePairs.begin(), wholePairs.end(), [&](const std::pair<KeyType, ValueType>& t1, const std::pair<KeyType, ValueType>& t2) {
      		return key_comparator_(t1.first,t2.first);});

  return std::vector<std::pair<KeyType, ValueType >>(wholePairs.begin()+std::distance(wholePairs.begin(), wholePairs.end())/2,wholePairs.end()); //TODO: optimize?
}


template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
checkIfRemoveDelta(Node* head){
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
    class ValueComparator, class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
splitPage(pid_t pPID,pid_t rPID,pid_t pParentPID){

  pid_t qPID;Node* splitNode;

  Node *headNodeP = mapping_table_.get_phy_ptr(pPID); //get the head node of the deltachain
  Node *headNodeParentP = mapping_table_.get_phy_ptr(pParentPID);

  if(checkIfRemoveDelta(headNodeP) || checkIfRemoveDelta(headNodeParentP))
    return false;

  KeyType Kp,Kq;

  if(headNodeP->is_leaf())
  {
    LeafNode* newLeafNode = new LeafNode(pPID,rPID); // P->R was there before
    qPID = static_cast<pid_t>(pid_gen_++);
    const std::vector<std::pair<KeyType, ValueType>>& qElemVec = getToBeMovedPairsLeaf(headNodeP);
    newLeafNode->key_values = qElemVec; //TODO: optimize?
    Kp = qElemVec[0].first;
    newLeafNode->record_count = qElemVec.size();
    Kq = qElemVec[newLeafNode->record_count-1].first;// TODO: Kq is max among all of P keys before split, if there is something greater than Kq coming
    // TODO: for insert then deltaInsert is created on top of Kp
    mapping_table_.insert_new_pid(qPID, newLeafNode);
    splitNode = new DeltaSplitLeaf(Kp, qPID, headNodeP->record_count-newLeafNode->record_count);
  }
  else
  {
    InnerNode* newInnerNode = new InnerNode(pPID,headNodeP->level,rPID);
    qPID = static_cast<pid_t>(pid_gen_++);
    //TODO: copy vector
    pid_t lastChild;
    std::vector<std::pair<KeyType, std::vector<pid_t> >> qElemVec = getToBeMovedPairsInner(headNodeP);
    Kp = qElemVec[0].first;
    lastChild = qElemVec[0].second.front();

    qElemVec.erase(qElemVec.begin()); //TODO: Write this cleanly
    newInnerNode->key_values = qElemVec; //TODO: optimize?
    newInnerNode->last_child = lastChild;
    newInnerNode->record_count = qElemVec.size();

    Kq = qElemVec[newInnerNode->record_count-1].first;//TODO: check if right?
    // Kq is max among all of P keys before split, if there is something greater than Kq coming
    // for insert then deltaInsert is created on top of Kp

    mapping_table_.insert_new_pid(qPID, newInnerNode);
    splitNode = new DeltaSplitInner(Kp, qPID, headNodeP->record_count-newInnerNode->record_count-1);//TODO: check if -1 is required as Kp is also to be removed
  }

  splitNode->set_next(headNodeP);
  if(!mapping_table_.install_node(pPID, headNodeP, splitNode))
    return false;

  //need to handle any specific delta record nodes?1

  //what if P disppears or the parent disappears, or R disappears
  setSibling(headNodeP,NULL_PID);

  Node* sepDel = new IndexDelta(Kp,Kq,qPID);
  sepDel->set_next(headNodeParentP);
  return !mapping_table_.install_node(pParentPID, headNodeParentP, sepDel) ? false : true;

}


    // step 1 marks start of merge, if fail on step 1 and see a RemoveNodeDelta,
// abort itself
// step 2 and step 3 should retry until succeed
// TODO key range
// DONE failure case
// DONE memory management
// TODO OPTIMIZATION reuse the nodes
template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
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
    class ValueComparator, class KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
consolidate(Node *node) {
  node->chain_length = 1;

}
// go through pid table, delete chain
// on merge node, delete right chain
template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
Cleanup() {
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

// Explicit template instantiation
template class BWTreeIndex<IntsKey<1>, ItemPointer, ItemPointerComparator, IntsComparator<1>,
    IntsEqualityChecker<1>>;
template class BWTreeIndex<IntsKey<2>, ItemPointer, ItemPointerComparator, IntsComparator<2>,
    IntsEqualityChecker<2>>;
template class BWTreeIndex<IntsKey<3>, ItemPointer, ItemPointerComparator, IntsComparator<3>,
    IntsEqualityChecker<3>>;
template class BWTreeIndex<IntsKey<4>, ItemPointer, ItemPointerComparator, IntsComparator<4>,
    IntsEqualityChecker<4>>;

template class BWTreeIndex<GenericKey<4>, ItemPointer, ItemPointerComparator, GenericComparator<4>,
    GenericEqualityChecker<4>>;
template class BWTreeIndex<GenericKey<8>, ItemPointer, ItemPointerComparator, GenericComparator<8>,
    GenericEqualityChecker<8>>;
template class BWTreeIndex<GenericKey<12>, ItemPointer, ItemPointerComparator, GenericComparator<12>,
    GenericEqualityChecker<12>>;
template class BWTreeIndex<GenericKey<16>, ItemPointer, ItemPointerComparator, GenericComparator<16>,
    GenericEqualityChecker<16>>;
template class BWTreeIndex<GenericKey<24>, ItemPointer, ItemPointerComparator, GenericComparator<24>,
    GenericEqualityChecker<24>>;
template class BWTreeIndex<GenericKey<32>, ItemPointer, ItemPointerComparator, GenericComparator<32>,
    GenericEqualityChecker<32>>;
template class BWTreeIndex<GenericKey<48>, ItemPointer, ItemPointerComparator, GenericComparator<48>,
    GenericEqualityChecker<48>>;
template class BWTreeIndex<GenericKey<64>, ItemPointer, ItemPointerComparator, GenericComparator<64>,
    GenericEqualityChecker<64>>;
template class BWTreeIndex<GenericKey<96>, ItemPointer, ItemPointerComparator, GenericComparator<96>,
    GenericEqualityChecker<96>>;
template class BWTreeIndex<GenericKey<128>, ItemPointer, ItemPointerComparator, GenericComparator<128>,
    GenericEqualityChecker<128>>;
template class BWTreeIndex<GenericKey<256>, ItemPointer, ItemPointerComparator, GenericComparator<256>,
    GenericEqualityChecker<256>>;
template class BWTreeIndex<GenericKey<512>, ItemPointer, ItemPointerComparator, GenericComparator<512>,
    GenericEqualityChecker<512>>;

template class BWTreeIndex<TupleKey, ItemPointer, ItemPointerComparator, TupleKeyComparator,
    TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
