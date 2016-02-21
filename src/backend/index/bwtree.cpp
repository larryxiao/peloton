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
			class KeyEqualityChecker>
	unsigned long BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
	node_key_search(const TreeNode *node, KeyType &key,
									const NodeSearchMode mode = NodeSearchMode::GTE) {

		auto last_key = node->key_values.back().first;
		// if lastKey < key?
		if(key_compare_lt(last_key , key))
			// return n+1 th position
			return node->key_values.size();

		// set the binary search range
		unsigned long min = 0, max = node->key_values.size() - 1;

		//used to store comparison result after switch case
		bool compare_result;

		while (min < max) {
			// find middle element
			unsigned long mid = (min + max) >> 1;

			// extract the middle key
			auto mid_key = node->key_values[mid].first;

			switch(mode){
				case GTE:
					compare_result = key_compare_lte(key, mid_key);
					break;
				case GT:
					compare_result = key_compare_lt(key, mid_key);
					break;
				default:
					//default case is GTE for now
					compare_result = key_compare_lte(key, mid_key);
			}

			if (compare_result) {
				max = mid;
			} else {
				min = mid+1;
			}
		}
		return min;
	}

	template <typename KeyType, typename ValueType, class KeyComparator,
			class KeyEqualityChecker>

	TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
			do_tree_operation(const pid_t node_pid, KeyType &key,
												ValueType *value,
												const LeafOperation *leaf_operation,
												const OperationType &op_type) {

		Node *node = mapping_table_.get_phy_ptr(node_pid);
		return do_tree_operation(node, key, value, leaf_operation, op_type);
	};

	template <typename KeyType, typename ValueType, class KeyComparator,
			class KeyEqualityChecker>

	TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
	do_tree_operation(Node* head, KeyType &key,
										ValueType *value,
										const LeafOperation *leaf_operation,
										const OperationType &op_type) {

		if (head->get_type() == NodeType::removeNode) {
			// we have a remove node delta, end search and try again
			return get_failed_result();
		}

		if (head->chain_length > consolidate_threshold_inner_) {
			consolidate(head);
		}

		// flag to indicate if iteration should be continued
		bool continue_itr = true;

		// store result from recursion, if any
		TreeOpResult result = get_failed_result();

		// pointer for iterating
		auto node = head;

		//iterate through the delta chain, based on case
		while (continue_itr && node != nullptr) {
			// Note: explicitly set continue_itr state only if it goes to false

			// switch on the type
			switch(node->get_type()) {
				case indexDelta: {
					// cast the node
					auto delta_node = static_cast<IndexDelta *>(node);
					// Kp <= key, key < Kq?
					if(key_compare_lte(delta_node->low, key) &&
							key_compare_lt(key, delta_node->high)) {
						// recurse into shortcut pointer
						result = do_tree_operation(delta_node->new_node,
																				key, value, leaf_operation,
																				op_type);
						// don't iterate anymore
						continue_itr = false;
					}
					break;
				}

				case deleteIndex: {
					auto delta_node = static_cast<DeleteIndex *>(node);
					// Kp <= key, key < Kq?
					if(key_compare_lte(delta_node->low, key) &&
							key_compare_lt(key, delta_node->high)) {
						// recurse into shortcut pointer
						result = do_tree_operation(delta_node->merge_node, key,
																				value, leaf_operation, op_type);
						// don't iterate anymore
						continue_itr = false;
					}
					break;
				}

				case deltaSplitInner: {
					auto delta_node = static_cast<DeltaSplitInner *>(node);
					// splitkey <= Key?
					if(key_compare_lte(delta_node->splitKey, key)){
						// recurse into new node
						result = do_tree_operation(delta_node->new_node, key, value,
																				leaf_operation, op_type);
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
																				value, leaf_operation, op_type);
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
						if (op_type == OperationType::search_op){
							// search the leaf page and get result
							result = leaf_operation->search_leaf_page(child_pid, key);
							// don't iterate anymore
							continue_itr = false;
						} else {
							// insert/delete operation; try to update
							// delta chain and fetch result
							result = leaf_operation->update_leaf_delta_chain(child_pid,
																																&key, value,
																																op_type);
							// don't iterate anymore
							continue_itr = false;
						}
					} else {
						// otherwise recurse into child node
						result = do_tree_operation(child_pid, key, value,
																			 leaf_operation, op_type);
						//don't iterate anymore
						continue_itr = false;
					}

					// check if child node has requested for merge
					if (result.needs_merge) {
						// TODO: Handle leftmost edge case, see WIKI

						// where we came from
						pid_t right = child_pid;

						// what we are merging with
						pid_t left =inner_node->key_values[child_pos-1].second;

						// Merge
						merge_page(left, right, inner_node->pid);
					}
					break;
				}
			}

			// move to the next node
			node = node->next;
		}


		result.needs_split = false;

		if (head->record_count > split_threshold_){
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
	TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
			search_leaf_page(const pid_t node_pid, const KeyType &key) {

		Node *head = mapping_table_.get_phy_ptr(node_pid);
		return search_leaf_page(head, key);
	};

	template <typename KeyType, typename ValueType, class KeyComparator,
			class KeyEqualityChecker>
	TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
	search_leaf_page(Node* head, const KeyType &key) {

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

		while(continue_itr && node != nullptr) {

			switch(node->get_type()) {

				case deltaInsert: {
					auto delta_node = static_cast<DeltaInsert *>(node);

					if (delta_node->key == key) {
						// node has been inserted, prepare result
						result = make_search_result(delta_node->value);

						continue_itr = false;
					}

					break;
				}

				case deltaDelete: {
					auto delta_node = static_cast<DeltaDelete *>(node);

					if (delta_node->key == key) {
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
			}
			// go to next node
			node = node->next;
		}

		result.needs_split = false;
		if (head->record_count > split_threshold_) {
			result.needs_split = true;
		}

		result.needs_merge = false;
		if (head->record_count < merge_threshold_) {
			result.needs_merge = true;
		}

		return result;

	}

	template <typename KeyType, typename ValueType, class KeyComparator,
			class KeyEqualityChecker>
	TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
	update_leaf_delta_chain(const pid_t pid, KeyType* key, ValueType* value,
															const OperationType& op_type) {

		Node *head = mapping_table_.get_phy_ptr(pid);

		if (head->get_type() == NodeType::removeNode) {
			return get_failed_result();
		}

		if (head->chain_length > consolidate_threshold_leaf_) {
			consolidate(head);
		}

		Node *update = nullptr;

		if (op_type == OperationType::insert_op) {
			update = new DeltaInsert(key, *value);
		} else {
			// otherwise, it is a delte op
			update = new DeltaDelete(key);
		}

		// try till update succeeds
		while(!mapping_table_.install_node(pid, head, update)){
			// get latest head value
			 head = mapping_table_.get_phy_ptr(pid);
		}

		// link to delta chain
		update->set_next(head);

		TreeOpResult result = get_update_success_result();

		result.needs_split = false;
		if (head->record_count > split_threshold_) {
			result.needs_split = true;
		}

		result.needs_merge = false;
		if (head->record_count < merge_threshold_) {
			result.needs_merge = true;
		}

		return result;

	}

	template <typename KeyType, typename ValueType, class KeyComparator,
			class KeyEqualityChecker>
	bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
	Search(const KeyType &key, ValueType **value) {
		LeafOperation leaf_op;
		// set the search leaf function
		leaf_op.search_leaf_page = search_leaf_page;

		auto result = do_tree_operation(root_, key, value, &leaf_op,
																		OperationType::search_op);

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
		LeafOperation leaf_op;
		// set the delta chain update function
		leaf_op.update_leaf_delta_chain = update_leaf_delta_chain;

		auto result = do_tree_operation(root_, key, value, &leaf_op,
																		OperationType::insert_op);

		return result.status;
	}


	template <typename KeyType, typename ValueType, class KeyComparator,
			class KeyEqualityChecker>
	bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
	Delete(const KeyType &key) {

		LeafOperation leaf_op;
		// set the delta chain update function
		leaf_op.update_leaf_delta_chain = update_leaf_delta_chain;

		auto result = do_tree_operation(root_, key, nullptr, &leaf_op,
																		OperationType::delete_op);

		return result.status;
	}


	template <typename KeyType, typename ValueType, class KeyComparator,
			class KeyEqualityChecker>
	void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
	merge_page(pid_t pid_l, pid_t pid_r, pid_t pid_parent) {

		Node *ptr_l = mapping_table_.get_phy_ptr(pid_l);
		Node *ptr_r = mapping_table_.get_phy_ptr(pid_r);
		Node *ptr_parent = mapping_table_.get_phy_ptr(pid_parent);
		bool leaf = ptr_r->is_leaf();

		// Step 1 marking for delete
		// create remove node delta node
		RemoveNode *remove_node_delta = new RemoveNode();
		remove_node_delta->set_next(ptr_r);
		mapping_table_.install_node(pid_r, ptr_r, remove_node_delta);

		// merge delta node ptr
		Node *node_merge_delta = nullptr;

		// Step 2 merging children
		// create node merge delta
		// TODO key range
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

		// add to the list
		remove_node_delta->set_next(ptr_l);
		mapping_table_.install_node(pid_l, ptr_l, node_merge_delta);

		// Step 3 parent update
		// create index term delete delta
		auto left_ptr = static_cast<TreeNode *>(ptr_l);
		KeyType left_low_key = left_ptr->key_values.front().first;

		auto right_ptr = static_cast<TreeNode *>(ptr_r);
		KeyType right_high_key = right_ptr->key_values.back().first;

		DeleteIndex *index_term_delete_delta = new DeleteIndex(left_low_key, right_high_key, pid_l);
		index_term_delete_delta->set_next(ptr_parent);
		mapping_table_.install_node(pid_parent, ptr_parent, (Node *) index_term_delete_delta);
	}

	template <typename KeyType, typename ValueType, class KeyComparator,
			class KeyEqualityChecker>
	bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Cleanup() {
		return true;
	}
}  // End index namespace
}  // End peloton namespace
