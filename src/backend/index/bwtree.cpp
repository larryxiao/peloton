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

namespace peloton {
namespace index {

	template <typename KeyType, typename ValueType, class KeyComparator,
			class KeyEqualityChecker>
	int BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
	node_key_search(const TreeNode *node, KeyType &key,
									const NodeSearchMode mode = NodeSearchMode::GTE) {

		// empty node? error case
		if(node->keys.size() == 0) return -1;

		// if lastKey < key?
		if(key_compare_lt(node->keys.back() , key))
			// return n+1 th position
			return node->keys.size();

		// set the binary search range
		int min = 0, max = node->keys.size() - 1;

		//used to store comparison result after switch case
		bool compare_result;

		while (min < max) {
			// find middle element
			int mid = (min + max) >> 1;

			switch(mode){
				case GTE:
					compare_result = key_compare_lte(key, node->keys[mid]);
					break;
				case GT:
					compare_result = key_compare_lt(key, node->keys[mid]);
					break;
				default:
					//default case is GTE for now
					compare_result = key_compare_lte(key, node->keys[mid]);
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
		TreeOpResult *result = nullptr;

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
						*result = do_tree_operation(delta_node->new_node,
																				key, value, leaf_operation,
																				op_type);
						// don't iterate anymore
						continue_itr = false;
					}
					// else, descend delta chain
				}

				case deleteIndex: {
					auto delta_node = static_cast<DeleteIndex *>(node);
					// Kp <= key, key < Kq?
					if(key_compare_lte(delta_node->low, key) &&
							key_compare_lt(key, delta_node->high)) {
						// recurse into shortcut pointer
						*result = do_tree_operation(delta_node->merge_node, key,
																				value, leaf_operation, op_type);
						// don't iterate anymore
						continue_itr = false;
					}
					// else, descend delta chain
				}

				case deltaSplitInner: {
					auto delta_node = static_cast<DeltaSplitInner *>(node);
					// splitkey <= Key?
					if(key_compare_lte(delta_node->splitKey, key)){
						// recurse into new node
						*result = do_tree_operation(delta_node->new_node, key, value,
																				leaf_operation, op_type);
						// don't iterate anymore
						continue_itr = false;
					}
					// else, descend delta chain
				}

				case mergeInner: {
					auto delta_node = static_cast<MergeInner *>(node);
					// splitkey <= key?
					if (key_compare_lte(delta_node->splitKey, key)) {
						// recurse into node to be deleted
						*result = do_tree_operation(delta_node->deleting_node, key,
																				value, leaf_operation, op_type);
						// don't iterate anymore
						continue_itr = false;
					}
					// else, descend delta chain
				}

				case inner: {
					auto inner_node = static_cast<InnerNode *>(node);
					// find the position of the child to the left of nearest
					// greater key
					int child_pos = node_key_search(inner_node, key);

					if (child_pos == -1) {
						//this shouldn't happen
						LOG_ERROR("Failed binary search at page");
						return get_failed_result();
					}

					auto child_pid = inner_node->children[child_pos];
					// check the node's level

					if (inner_node->level == 1) {
						// next level is leaf, execute leaf operation
						if (op_type == OperationType::search_op){
							// search the leaf page and get result
							*result = leaf_operation->search_leaf_page(child_pid, key);
							// don't iterate anymore
							continue_itr = false;
						} else {
							// insert/delete operation; try to update
							// delta chain and fetch result
							*result = leaf_operation->update_leaf_delta_chain(child_pid,
																																&key, value,
																																op_type);
							// don't iterate anymore
							continue_itr = false;
						}
					} else {
						// otherwise recurse into child node
						*result = do_tree_operation(inner_node->children[child_pid],
																				key, value, leaf_operation, op_type);
						//don't iterate anymore
						continue_itr = false;
					}

					if (result == nullptr) {
						LOG_ERROR("No valid result from operation");
					}
				}
			}

			// move to the next node
			node = node->next;
		}

		if (head->record_count > split_threshold_){
			// split this node
		}

		if (head-> record_count < merge_threshold_) {
			// merge this node
		}

		if(result == nullptr) {
			// this should not happen
			return get_failed_result();
		}

		// return the result from lower levels
		return result;

	}
}  // End index namespace
}  // End peloton namespace
